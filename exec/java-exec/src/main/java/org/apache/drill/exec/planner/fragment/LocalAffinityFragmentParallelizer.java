/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.fragment;

    import com.google.common.collect.Iterators;
    import com.google.common.collect.Lists;
    import com.google.common.collect.Ordering;
    import org.apache.drill.exec.physical.EndpointAffinity;
    import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
    import org.apache.drill.exec.proto.CoordinationProtos;
    import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

    import java.util.Collection;
    import java.util.Collections;
    import java.util.Comparator;
    import java.util.Iterator;
    import java.util.List;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Set;
    import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation of {@link FragmentParallelizer} where fragment has zero or more endpoints with affinities. Width
 * per node is depended on the affinity to the endpoint and total width (calculated using costs). Based on various
 * factors endpoints which have no affinity can be assigned to run the fragments.
 */
public class LocalAffinityFragmentParallelizer implements FragmentParallelizer {
    public static final LocalAffinityFragmentParallelizer INSTANCE = new LocalAffinityFragmentParallelizer();

    private static final Ordering<EndpointAffinity> ENDPOINT_AFFINITY_ORDERING =
        Ordering.from(new Comparator<EndpointAffinity>() {
            @Override
            public int compare(EndpointAffinity o1, EndpointAffinity o2) {
                // Sort in descending order of affinity values
                return Double.compare(o2.getAffinity(), o1.getAffinity());
            }
        });

    @Override
    public void parallelizeFragment(final Wrapper fragmentWrapper, final ParallelizationParameters parameters, final Collection<CoordinationProtos.DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
        final Fragment fragment = fragmentWrapper.getNode();

        // Find the parallelization width of fragment
        final Stats stats = fragmentWrapper.getStats();
        final ParallelizationInfo parallelizationInfo = stats.getParallelizationInfo();

        // 1. Find the parallelization based on cost. Use max cost of all operators in this fragment; this is consistent
        //    with the calculation that ExcessiveExchangeRemover uses.
        int width = (int) Math.ceil(stats.getMaxCost() / parameters.getSliceTarget());

        // 2. Cap the parallelization width by fragment level width limit and system level per query width limit
        width = Math.min(width, Math.min(parallelizationInfo.getMaxWidth(), parameters.getMaxGlobalWidth()));

        // 3. Cap the parallelization width by system level per node width limit
        // width = Math.min(width, parameters.getMaxWidthPerNode() * activeEndpoints.size());

        // 4. Make sure width is at least the min width enforced by operators
        width = Math.max(parallelizationInfo.getMinWidth(), width);

        // 4. Make sure width is at most the max width enforced by operators
        width = Math.min(parallelizationInfo.getMaxWidth(), width);

        // 5 Finally make sure the width is at least one
        width = Math.max(1, width);

        fragmentWrapper.setWidth(width);

        final List<CoordinationProtos.DrillbitEndpoint> assignedEndpoints = findEndpoints(activeEndpoints,
              parallelizationInfo.getNumEndpointAssignments(), parallelizationInfo.getEndpointAffinityMap(), fragmentWrapper.getWidth(), parameters);

        fragmentWrapper.assignEndpoints(assignedEndpoints);

    }


    // Assign endpoints based on the given endpoint list, affinity map and width.
    private List<DrillbitEndpoint> findEndpoints(final Collection<DrillbitEndpoint> activeEndpoints,
                                                 final Map<DrillbitEndpoint, Integer> numEndpointAssignments,
                                                 final Map<DrillbitEndpoint, EndpointAffinity> endpointAffinityMap,
                                                 final int width, final ParallelizationParameters parameters) throws PhysicalOperatorSetupException {

        final List<DrillbitEndpoint> endpoints = Lists.newArrayList();

        Iterator<DrillbitEndpoint> EPItr = Iterators.cycle(activeEndpoints);

        Map<DrillbitEndpoint, Integer> numEndpointAssignmentsCopy = new HashMap<>();
        for(Map.Entry<DrillbitEndpoint, Integer> e : numEndpointAssignments.entrySet()) {
            if(!numEndpointAssignmentsCopy.containsKey(e.getKey())) {
                numEndpointAssignmentsCopy.put(e.getKey(), e.getValue());
            }
        }

        if (endpointAffinityMap.size() > 0) {
            // Get EndpointAffinity list sorted in descending order of affinity values
            List<EndpointAffinity> sortedAffinityList = ENDPOINT_AFFINITY_ORDERING.immutableSortedCopy(endpointAffinityMap.values());

            // Find the number of mandatory nodes (nodes with +infinity affinity).
            int numRequiredNodes = 0;
            for (EndpointAffinity ep : sortedAffinityList) {
                if (ep.isAssignmentRequired()) {
                    numRequiredNodes++;
                    endpoints.add(ep.getEndpoint());
                    if(numEndpointAssignmentsCopy.containsKey(ep.getEndpoint())) {
                        numEndpointAssignmentsCopy.put(ep.getEndpoint(), numEndpointAssignmentsCopy.get(ep.getEndpoint()) - 1);
                    }
                }
            }

            if (width < numRequiredNodes) {
                throw new PhysicalOperatorSetupException("Can not parallelize the fragment as the parallelization width (" + width + ") is " + "less than the number of mandatory nodes (" + numRequiredNodes + " nodes with +INFINITE affinity).");
            }

        }

        // Keep adding until we have selected "width" number of endpoints.
        while (endpoints.size() < width) {
            DrillbitEndpoint endpoint = EPItr.next();
            if (numEndpointAssignmentsCopy.containsKey(endpoint) && numEndpointAssignmentsCopy.get(endpoint) > 0) {
                endpoints.add(endpoint);
                numEndpointAssignmentsCopy.put(endpoint, numEndpointAssignmentsCopy.get(endpoint) - 1);
            }
        }

        return endpoints;
    }

}
