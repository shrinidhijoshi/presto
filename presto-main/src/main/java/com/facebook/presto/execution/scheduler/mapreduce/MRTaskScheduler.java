/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler.mapreduce;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.operator.ForScheduler;
import com.google.common.collect.Sets;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MRTaskScheduler
{
    private static final Logger logger = Logger.get(MRTaskScheduler.class);
    private static final int SLOTS_PER_NODE = 1;
    private final MRTaskQueue httpRemoteTaskQueue;
    private final InternalNodeManager internalNodeManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<InternalNode, InternalNodeState> nodeTasksMap = new ConcurrentHashMap<>();
    private InternalNode coordinator;

    @Inject
    public MRTaskScheduler(
            MRTaskQueue httpRemoteTaskQueue,
            InternalNodeManager internalNodeManager,
            @ForScheduler ScheduledExecutorService scheduledExecutorService)
    {
        this.httpRemoteTaskQueue = httpRemoteTaskQueue;
        this.internalNodeManager = internalNodeManager;
        this.scheduledExecutorService = scheduledExecutorService;
        this.internalNodeManager.addNodeChangeListener(this::updateSlotStates);
    }

    @PostConstruct
    public void start()
    {
        scheduledExecutorService.scheduleAtFixedRate(this::refreshSlotStates, 0, 2, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void close()
    {
        logger.error("QueueingTaskAssigner is being closed !!");
    }

    private void updateSlotStates(AllNodes allNodes)
    {
        // newNodes = newSlots
        allNodes.getActiveNodes().forEach(internalNode -> {
            if (internalNode.isCatalogServer() || internalNode.isResourceManager()) {
                return;
            }
            // we track coordinator separately to run the
            // COORDINATOR_ONLY tasks
            if (internalNode.isCoordinator()) {
                coordinator = internalNode;
                return;
            }

            nodeTasksMap.putIfAbsent(internalNode, new InternalNodeState(internalNode, ResourceNodeState.ALIVE, SLOTS_PER_NODE));
        });

        // remove dead Nodes from future scheduling on their slots
        // the tasks running on them could reach any of below state
        // 1. LOST this is because something crashed the node before task completed
        // 2. FAILED this is because the task failed and the then the node crashed
        // 3. FINISHED this is because the task succeeded but the node crashed after right after
        // We do not explicitly handle these states here as the task polling will do it for us.
        nodeTasksMap.forEach((key, value) -> {
            if (allNodes.getActiveNodes().contains(key)) {
                return;
            }
            // node is dead
            nodeTasksMap.get(key).setState(ResourceNodeState.DEAD);
            nodeTasksMap.get(key).getSlots().forEach(resourceSlot -> {
                if (resourceSlot.isOccupied() && resourceSlot.getRemoteTask().getTaskStatus().getState().isDone()) {
                    logger.error("Node is dead but task is still not done.");
                }
            });
        });

        refreshSlotStates();
    }

    public void refreshSlotStates()
    {
        synchronized (this) {
            // free finished slots
            // update slot states based on task
            nodeTasksMap.values().forEach(InternalNodeState::refreshSlotStates);

            // get open slots
            Set<ResourceSlot> slots = nodeTasksMap.values().stream()
                    .filter(InternalNodeState::isAlive)
                    .flatMap(internalNodeState -> internalNodeState.getSlots().stream())
                    .collect(Collectors.toSet());

            logger.info(
                    "\n" +
                            "OCCUPIED SLOTS = %s \n" +
                            "OPEN     SLOTS = %s \n" +
                            "QUEUED   TASKS = %s",
                    slots.stream().filter(ResourceSlot::isOccupied).map(ResourceSlot::toString).collect(Collectors.joining(" . ")),
                    slots.stream().filter(resourceSlot -> !resourceSlot.isOccupied()).collect(Collectors.toSet()).size(),
                    httpRemoteTaskQueue.size());
        }
    }

    /**
     * This is Step5 of MRQueryScheduler
     */
    public void assignTasksBasedOnAvailableResources()
    {
        synchronized (this) {
            try {
                // get open slots
                Set<ResourceSlot> slots = nodeTasksMap.values().stream()
                        .filter(InternalNodeState::isAlive)
                        .flatMap(internalNodeState -> internalNodeState.getSlots().stream())
                        .collect(Collectors.toSet());

                logger.info(
                        "OCCUPIED SLOTS = %s \n" +
                        "OPEN     SLOTS = %s, \n" +
                        "QUEUED   TASKS = %s",
                        slots.stream().filter(ResourceSlot::isOccupied).map(ResourceSlot::toString).collect(Collectors.joining(" . ")),
                        slots.stream().filter(resourceSlot -> !resourceSlot.isOccupied()).collect(Collectors.toSet()).size(),
                        httpRemoteTaskQueue.size());

                // pick from queue and start assigning them to slots
                slots.stream().filter(resourceSlot -> !resourceSlot.isOccupied()).forEach(resourceSlot -> {
                    if (httpRemoteTaskQueue.size() == 0) {
                        return;
                    }

                    RemoteTask remoteTask = null;
                    try {
                        remoteTask = httpRemoteTaskQueue.poll();
                    }
                    catch (Exception ex) {
                        logger.error("Error polling task from taskQueue");
                    }

                    if (remoteTask != null) {
                        if (remoteTask.getPlanFragment().getPartitioning().isCoordinatorOnly()) {
                            remoteTask.assignToNode(coordinator, null);
                            remoteTask.start();
                            return;
                        }

                        try {
                            // Try to assign the task to that slot
                            if (!tryScheduleTaskOnSlot(remoteTask, resourceSlot)) {
                                logger.error("Failed to assign task to slot. Putting it back in queue");
                                httpRemoteTaskQueue.addTask(remoteTask);
                            }
                            else {
                                logger.info("Assigned task %s to node %s",
                                        remoteTask.getTaskId(), resourceSlot.getInternalNode().getNodeIdentifier());
                            }
                        }
                        catch (Exception ex) {
                            logger.error("Exception trying to schedule task %s. Will put it back to queue",
                                    remoteTask.getTaskId());
                            httpRemoteTaskQueue.addTask(remoteTask);
                        }
                    }
                    else {
                        logger.warn("No tasks found for queueing");
                    }
                });
            }
            catch (Throwable t) {
                logger.error("Error in assigner loop", t.getMessage());
            }
        }
    }

    public static class ResourceSlot
    {
        private final AtomicReference<RemoteTask> task = new AtomicReference<>();
        private final InternalNode internalNode;

        public ResourceSlot(
                InternalNode internalNode)
        {
            this.internalNode = internalNode;
        }

        public synchronized boolean occupy(RemoteTask task)
        {
            return this.task.compareAndSet(null, task);
        }

        public boolean isOccupied()
        {
            return this.task.get() != null;
        }

        public RemoteTask getRemoteTask()
        {
            return task.get();
        }

        public void free()
        {
            task.set(null);
        }

        public InternalNode getInternalNode()
        {
            return internalNode;
        }

        public String toString()
        {
            String taskId = isOccupied() ? task.get().getTaskId().toString() : " - ";
            return getInternalNode().getNodeIdentifier() + "[" + taskId + "]";
        }
    }

    public static class InternalNodeState
    {
        private final List<ResourceSlot> slots;
        private final Set<RemoteTask> succeededTasks;
        private final Set<RemoteTask> failedTasks;
        private final Set<RemoteTask> lostTasks;

        private ResourceNodeState state;
        public InternalNodeState(
                InternalNode internalNode,
                ResourceNodeState state,
                int resourceSlotsCount)
        {
            this.state = state;
            slots = IntStream.range(0, resourceSlotsCount)
                    .mapToObj(i -> new ResourceSlot(internalNode))
                    .collect(Collectors.toList());
            succeededTasks = Sets.newConcurrentHashSet();
            failedTasks = Sets.newConcurrentHashSet();
            lostTasks = Sets.newConcurrentHashSet();
        }

        public List<ResourceSlot> getSlots()
        {
            return slots;
        }

        public void refreshSlotStates()
        {
            slots.stream().filter(ResourceSlot::isOccupied).forEach(slot -> {
                RemoteTask task = slot.getRemoteTask();
                switch (task.getTaskStatus().getState()) {
                    case PLANNED:
                    case RUNNING:
                        break;
                    case FAILED:
                    case ABORTED:
                    case CANCELED:
                        failedTasks.add(task);
                        slot.free();
                        break;
                    case FINISHED:
                        succeededTasks.add(task);
                        slot.free();
                        break;
                }
            });
        }

        public void setState(ResourceNodeState state)
        {
            this.state = state;
        }

        public boolean isAlive()
        {
            return ResourceNodeState.ALIVE == this.state;
        }
    }

    public enum ResourceNodeState {
        ALIVE(0),
        PROBABLY_DEAD(1),
        DEAD(2);
        private final int state;

        ResourceNodeState(int state)
        {
            this.state = state;
        }
    }

    public static boolean tryScheduleTaskOnSlot(RemoteTask task, ResourceSlot slot)
    {
        if (slot.isOccupied()) {
            logger.error("Trying to schedule task into occupied slot");
            return false;
        }

        slot.occupy(task);
        try {
            // compute remaining task details based on node info
            task.assignToNode(
                    slot.getInternalNode(),
                    null);
            // try starting the task
            task.start();
        }
        catch (Exception ex) {
            logger.error("Failed to schedule task on slot. Freeing the slot.", ex.getStackTrace());
            slot.free();
            return false;
        }

        return true;
    }
}
