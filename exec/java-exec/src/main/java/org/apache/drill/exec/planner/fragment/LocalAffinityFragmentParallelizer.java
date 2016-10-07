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

import com.google.common.collect.Lists;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;


/**
 * Implementation of {@link FragmentParallelizer} where fragment has zero or more endpoints.
 * This is for Parquet Scan Fragments only. Fragment placement is done preferring strict
 * data locality.
 */
public class LocalAffinityFragmentParallelizer implements FragmentParallelizer {
    public static final LocalAffinityFragmentParallelizer INSTANCE = new LocalAffinityFragmentParallelizer();

    @Override
    public void parallelizeFragment(final Wrapper fragmentWrapper, final ParallelizationParameters parameters, final Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {

        // Find the parallelization width of fragment
        final Stats stats = fragmentWrapper.getStats();
        final ParallelizationInfo parallelizationInfo = stats.getParallelizationInfo();

        // 1. Find the parallelization based on cost. Use max cost of all operators in this fragment; this is consistent
        //    with the calculation that ExcessiveExchangeRemover uses.
        int width = (int) Math.ceil(stats.getMaxCost() / parameters.getSliceTarget());

        // 2. Cap the parallelization width by fragment level width limit and system level per query width limit
        width = Math.min(width, Math.min(parallelizationInfo.getMaxWidth(), parameters.getMaxGlobalWidth()));

        // 3. Cap the parallelization width by system level per node width limit
        width = Math.min(width, parameters.getMaxWidthPerNode() * activeEndpoints.size());

        // 4. Make sure width is at least the min width enforced by operators
        width = Math.max(parallelizationInfo.getMinWidth(), width);

        // 5. Make sure width is at most the max width enforced by operators
        width = Math.min(parallelizationInfo.getMaxWidth(), width);

        // 6: Finally make sure the width is at least one
        width = Math.max(1, width);

        List<DrillbitEndpoint> endpointPool = Lists.newArrayList();
        List<DrillbitEndpoint> assignedEndPoints = Lists.newArrayList();

        Map<DrillbitEndpoint, EndpointAffinity> endpointAffinityMap =
            fragmentWrapper.getStats().getParallelizationInfo().getEndpointAffinityMap();

        int totalAssigned = 0;
        int totalWorkUnits = 0;

        // Get the total number of work units and list of endPoints to schedule fragments on
        for (Map.Entry<DrillbitEndpoint, EndpointAffinity> epAff : endpointAffinityMap.entrySet()) {
            if (epAff.getValue().getNumLocalWorkUnits() > 0) {
                totalWorkUnits += epAff.getValue().getNumLocalWorkUnits();
                endpointPool.add(epAff.getKey());
            }
        }

        // Keep track of number of fragments allocated to each endpoint.
        Map<DrillbitEndpoint, Integer> endpointAssignments = new HashMap<>();

        // Keep track of how many more to assign to each endpoint.
        Map<DrillbitEndpoint, Integer> remainingEndpointAssignments = new HashMap<>();

        // Calculate the target allocation for each endPoint based on work it has to do
        // Assign one fragment (minimum) to all the endPoints in the pool.
        for (DrillbitEndpoint ep : endpointPool) {
            int targetAllocation = (int) Math.ceil(endpointAffinityMap.get(ep).getNumLocalWorkUnits() * width / parallelizationInfo.getMaxWidth());
            assignedEndPoints.add(ep);
            totalAssigned++;
            remainingEndpointAssignments.put(ep, targetAllocation-1);
            endpointAssignments.put(ep, 1);
        }

        // Keep allocating from endpoints in a round robin fashion upto
        // max(targetAllocation, maxwidthPerNode) for each endpoint and
        // upto width for all together.
        while(totalAssigned < width) {
            int assignedThisRound = 0;
            for (DrillbitEndpoint ep : endpointPool) {
                if (remainingEndpointAssignments.get(ep) > 0 &&
                    remainingEndpointAssignments.get(ep) < parameters.getMaxWidthPerNode()) {
                    assignedEndPoints.add(ep);
                    remainingEndpointAssignments.put(ep, remainingEndpointAssignments.get(ep) - 1);
                    totalAssigned++;
                    assignedThisRound++;
                    endpointAssignments.put(ep, endpointAssignments.get(ep) + 1);
                }
                if (totalAssigned == width) {
                    break;
                }
            }
            if (assignedThisRound == 0) {
                break;
            }
        }

        // This is for the case where drillbits are not running on endPoints which have data.
        // Allocate them from the active endpoint pool.
        int totalUnAssigned =
            (parallelizationInfo.getMaxWidth() - totalWorkUnits) * width/parallelizationInfo.getMaxWidth();

        while (totalAssigned < width && totalUnAssigned > 0) {
            for (DrillbitEndpoint ep : activeEndpoints) {
                if (endpointAssignments.containsKey(ep) &&
                    endpointAssignments.get(ep) >= parameters.getMaxWidthPerNode()) {
                    continue;
                }
                assignedEndPoints.add(ep);
                totalAssigned++;
                totalUnAssigned--;
                if (endpointAssignments.containsKey(ep)) {
                    endpointAssignments.put(ep, endpointAssignments.get(ep) + 1);
                } else {
                    endpointAssignments.put(ep, 1);
                }
                if (totalUnAssigned == 0 || totalAssigned == width) {
                    break;
                }
            }
        }

        fragmentWrapper.setWidth(assignedEndPoints.size());
        fragmentWrapper.assignEndpoints(assignedEndPoints);
    }
}
