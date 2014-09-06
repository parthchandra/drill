/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work;

import com.google.common.collect.Maps;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;

import java.util.Map;

public class FragmentConnectionManager {
  private final Map<FragmentHandle, UserClientConnection> pendingCnxs = Maps.newConcurrentMap();
  private final Map<FragmentHandle, ThreadHolder> waitingThreads = Maps.newConcurrentMap();
  private final Object lock = new Object();

  public boolean setConnection(FragmentHandle handle, UserClientConnection connection) {
    ThreadHolder threadHolder;
    synchronized (lock) {
      pendingCnxs.put(handle, connection);
      threadHolder = waitingThreads.remove(handle);
    }

    if (threadHolder != null) {
      synchronized (threadHolder) {
        threadHolder.notify();
      }
      return true;
    }

    return false;
  }

  public UserClientConnection getConnection(FragmentHandle handle) {
    ThreadHolder threadHolder;
    synchronized (lock) {
      UserClientConnection clientConnection = pendingCnxs.remove(handle);
      if (clientConnection != null) {
        return clientConnection;
      }

      threadHolder = new ThreadHolder(Thread.currentThread());
      waitingThreads.put(handle, threadHolder);
    }

    try {
      synchronized (threadHolder) {
        threadHolder.wait();
      }
    } catch(InterruptedException e) {
      throw new RuntimeException(e);
    }

    return pendingCnxs.remove(handle);
  }

  private class ThreadHolder {
    private Thread t;

    ThreadHolder(Thread t) {
      this.t = t;
    }

    Thread getThread() {
      return t;
    }
  }
}
