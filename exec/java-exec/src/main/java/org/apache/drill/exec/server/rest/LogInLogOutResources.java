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

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.server.mvc.Viewable;

@Path("/")
@PermitAll
public class LogInLogOutResources {
  @GET
  @Path("/log/in")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPage() {
    return ViewableWithPermissions.createLoginPage(null);
  }

  // Request type is POST because POST request which contains the login credentials are invalid and the request is
  // dispatched here directly.
  @POST
  @Path("/log/error")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getLoginPageAfterValidationError() {
    return ViewableWithPermissions.createLoginPage("Invalid username/password credentials.");
  }

  @GET
  @Path("/log/out")
  @Produces(MediaType.TEXT_HTML)
  public Viewable logout(@Context HttpServletRequest req) {
    final HttpSession session = req.getSession();
    if (session != null) {
      session.invalidate();
    }

    return getLoginPage();
  }
}
