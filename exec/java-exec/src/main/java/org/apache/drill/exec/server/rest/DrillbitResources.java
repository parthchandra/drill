/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.codehaus.jackson.annotate.JsonCreator;
import org.glassfish.jersey.server.mvc.Viewable;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

@Path("/")
@PermitAll
public class DrillbitResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject SecurityContext sc;
  @Inject WorkManager work;

  @GET
  @Path("/drillbits.json")
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.APPLICATION_JSON)
  public List<DrillbitWrapper> getDrillbitsJSON() {
    List<DrillbitWrapper> drillbitsList = new LinkedList<>();
    ClusterCoordinator coord = work.getContext().getClusterCoordinator();
    for (CoordinationProtos.DrillbitEndpoint drillbit : coord.getAvailableEndpoints()) {
      drillbitsList.add(new DrillbitWrapper(drillbit));
    }
    return drillbitsList;
  }

  @GET
  @Path("/drillbits")
  @RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
  @Produces(MediaType.TEXT_HTML)
  public Viewable getDrillbits() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/drillbits.ftl", sc,
        getDrillbitsJSON());
  }


  @XmlRootElement
  public class DrillbitWrapper {

    private String address;
    private int controlPort;
    private int dataPort;
    private int userPort;

    @JsonCreator
    public DrillbitWrapper(CoordinationProtos.DrillbitEndpoint endpoint) {
      this.address = endpoint.getAddress();
      this.controlPort = endpoint.getControlPort();
      this.dataPort = endpoint.getDataPort();
      this.userPort = endpoint.getUserPort();
    }

    public String getAddress() {
      return this.address;
    }

    public int getControlPort() {
      return this.controlPort;
    }

    public int getDataPort() {
      return this.dataPort;
    }

    public int getUserPort() {
      return this.userPort;
    }
  }

}
