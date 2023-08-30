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

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.discovery.client.ServiceType;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.metadata.ForNodeManager;
import com.facebook.presto.metadata.HttpRemoteNodeState;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.RemoteNodeState;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.Sets;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodePoolType.DEFAULT;
import static com.facebook.presto.spi.NodeState.ACTIVE;

public class MRTaskScheduler
{
    private static final Logger logger = Logger.get(MRTaskScheduler.class);
    private static final int SLOTS_PER_NODE = 1;
    private final MRTaskQueue httpRemoteTaskQueue;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<String, InternalNodeState> nodeIdToNodeStateMap = new ConcurrentHashMap<>();
    private final ServiceSelector serviceSelector;
    private final HttpClient httpClient;
    private InternalNode coordinator;
    private final Set<Heartbeat> heartbeats;

    private long lastSchedulingEpoch;

    @Inject
    public MRTaskScheduler(
            @ServiceType("presto") ServiceSelector serviceSelector,
            MRTaskQueue httpRemoteTaskQueue,
            @ForScheduler ScheduledExecutorService scheduledExecutorService,
            @ForNodeManager HttpClient httpClient)
    {
        this.httpRemoteTaskQueue = httpRemoteTaskQueue;
        this.scheduledExecutorService = scheduledExecutorService;
        this.serviceSelector = serviceSelector;
        this.httpClient = httpClient;
        this.heartbeats = new HashSet<>();
    }

    @PostConstruct
    public void start()
    {
        scheduledExecutorService.scheduleAtFixedRate(this::processHeartbeats, 0, 1, TimeUnit.SECONDS);
    }

    public void postHeartbeats(Set<Heartbeat> heartbeats)
    {
        synchronized (this.heartbeats) {
            this.heartbeats.addAll(heartbeats);
        }
    }

    @PreDestroy
    public void close()
    {
        logger.error("QueueingTaskAssigner is being closed !!");
    }

    public void processHeartbeats()
    {
        synchronized (this.heartbeats) {
            Iterator<Heartbeat> it = heartbeats.iterator();
            while (it.hasNext()) {
                Heartbeat heartbeat = it.next();
                it.remove(); //dequeue the heartbeat

                logger.info("Heartbeat: %s", heartbeat.getNodeId());
                String nodeId = heartbeat.getNodeId();

                // if it's a new node then start tracking it
                if (!nodeIdToNodeStateMap.containsKey(nodeId)) {
                    URI nodeUri = URI.create(heartbeat.getUri());
                    nodeIdToNodeStateMap.put(nodeId,
                            new InternalNodeState(
                                    new InternalNode(
                                            nodeId,
                                            nodeUri,
                                            OptionalInt.of(nodeUri.getPort()),
                                            getNodeVersion(),
                                            false,
                                            false,
                                            false,
                                            ALIVE,
                                            OptionalInt.empty(),
                                            getPoolType()),
                                    SLOTS_PER_NODE,
                                    new HttpRemoteNodeState(
                                            httpClient,
                                            uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build())));
                }

                // if it is existing node then, "refresh" its slot states and set for receiving tasks
                InternalNodeState internalNodeState = nodeIdToNodeStateMap.get(nodeId);
                internalNodeState.getRemoteNodeState().asyncRefresh();
                internalNodeState.refreshSlotStates();
                internalNodeState.setLastHeartbeat(System.currentTimeMillis());
            }
        }
    }

    /**
     * This is Step5 of MRQueryScheduler
     */
    public void assignTasksBasedOnAvailableResources()
    {
        // make sure coordinator is assigned
        if (coordinator == null) {
            // get all current heartbeats
            Optional<ServiceDescriptor> coordinatorService = serviceSelector.selectAllServices().stream()
                    .filter(serviceDescriptor -> isCoordinator(serviceDescriptor))
                    .findFirst();

            coordinator = new InternalNode(
                coordinatorService.get().getNodeId(),
                getHttpUri(coordinatorService.get(), false),
                getThriftServerPort(coordinatorService.get()),
                getNodeVersion(),
                true,
                false,
                false,
                ALIVE,
                OptionalInt.empty(),
                getPoolType());
        }
        // schedule any coordinator tasks
        if (httpRemoteTaskQueue.tasksInCoordinatorQueue() > 0) {
            try {
                RemoteTask remoteTask = httpRemoteTaskQueue.pollCoordinatorTask();
                remoteTask.assignToNode(coordinator, null);
                remoteTask.start();
            }
            catch (InterruptedException ex) {
                logger.warn("wrongly assumed waiting coordinator tasks. There were none");
            }
        }

        synchronized (this) {
            try {
                // get nodes that had heartbeat since last epoch ("fresh"/"active" nodes)
                Set<ResourceSlot> slots = nodeIdToNodeStateMap.values().stream()
                        .filter(internalNodeState -> !internalNodeState.getInternalNode().isCoordinator())
                        .filter(internalNodeState -> internalNodeState.getLastHeartbeat() > lastSchedulingEpoch)
                        .filter(internalNodeState -> {
                            Optional<NodeState> remoteNodeState = internalNodeState.getRemoteNodeState().getNodeState();
                            return remoteNodeState.isPresent() && remoteNodeState.get().equals(ACTIVE);
                        })
                        .flatMap(internalNodeState -> internalNodeState.getSlots().stream())
                        .collect(Collectors.toSet());
                // reset scheduling epoch
                lastSchedulingEpoch = System.currentTimeMillis();

                logger.info(
                        "OCCUPIED SLOTS = %s \n" +
                        "OPEN     SLOTS = %s, \n" +
                        "QUEUED   TASKS = %s",
                        slots.stream().filter(ResourceSlot::isOccupied).map(ResourceSlot::toString).collect(Collectors.joining(" . ")),
                        slots.stream().filter(resourceSlot -> !resourceSlot.isOccupied()).collect(Collectors.toSet()).size(),
                        httpRemoteTaskQueue.tasksInQueue());

                logger.info("\nScheduling for nodes\n", slots.stream()
                                .map(resourceSlot -> resourceSlot.getInternalNode().getNodeIdentifier())
                                .collect(Collectors.joining(" \n ")));

                // pick from queue and start assigning them to slots
                slots.stream().filter(resourceSlot -> !resourceSlot.isOccupied()).forEach(resourceSlot -> {
                    if (httpRemoteTaskQueue.tasksInQueue() == 0) {
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
                throw t;
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
        private final InternalNode internalNode;
        private final List<ResourceSlot> slots;
        private final Set<RemoteTask> succeededTasks;
        private final Set<RemoteTask> failedTasks;
        private final Set<RemoteTask> lostTasks;
        private final AtomicLong lastHeartbeat;

        private final RemoteNodeState remoteNodeState;

        public InternalNodeState(
                InternalNode internalNode,
                int resourceSlotsCount,
                RemoteNodeState remoteNodeState)
        {
            this.internalNode = internalNode;
            slots = IntStream.range(0, resourceSlotsCount)
                    .mapToObj(i -> new ResourceSlot(internalNode))
                    .collect(Collectors.toList());
            succeededTasks = Sets.newConcurrentHashSet();
            failedTasks = Sets.newConcurrentHashSet();
            lostTasks = Sets.newConcurrentHashSet();
            this.remoteNodeState = remoteNodeState;
            lastHeartbeat = new AtomicLong(System.currentTimeMillis());
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

        public void refreshNodeState()
        {
            remoteNodeState.asyncRefresh();
        }

        public InternalNode getInternalNode()
        {
            return internalNode;
        }

        public RemoteNodeState getRemoteNodeState()
        {
            return remoteNodeState;
        }

        public long getLastHeartbeat()
        {
            return lastHeartbeat.get();
        }

        public void setLastHeartbeat(long value)
        {
            lastHeartbeat.set(value);
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

    private static NodePoolType getPoolType()
    {
        return DEFAULT;
//        if (!service.getProperties().containsKey(POOL_TYPE)) {
//            return DEFAULT;
//        }
//        return NodePoolType.valueOf(service.getProperties().get(POOL_TYPE));
    }

    private static URI getHttpUri(ServiceDescriptor descriptor, boolean httpsRequired)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }

    private static OptionalInt getThriftServerPort(ServiceDescriptor descriptor)
    {
        String port = descriptor.getProperties().get("thriftServerPort");
        if (port != null) {
            try {
                return OptionalInt.of(Integer.parseInt(port));
            }
            catch (IllegalArgumentException ignored) {
            }
        }
        return OptionalInt.empty();
    }

    private static OptionalInt getRaftPort(ServiceDescriptor descriptor)
    {
        String port = descriptor.getProperties().get("raftPort");
        if (port != null) {
            try {
                return OptionalInt.of(Integer.parseInt(port));
            }
            catch (IllegalArgumentException exception) {
                logger.error("Error getting raft port %s", port);
            }
        }
        return OptionalInt.empty();
    }

    private static NodeVersion getNodeVersion()
    {
        String nodeVersion = "test"; //descriptor.getProperties().get("node_version");
        return nodeVersion == null ? null : new NodeVersion(nodeVersion);
    }

    private static boolean isCoordinator(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("coordinator"));
    }

    private static boolean isResourceManager(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("resource_manager"));
    }

    private static boolean isCatalogServer(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("catalog_server"));
    }
}
