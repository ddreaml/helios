/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.secretprovider;

/**
 * A factory for {@link SecretProvider} instances, and the entry point for secret provider
 * plugins. {@link SecretProviderLoader} loads plugins by using {@link java.util.ServiceLoader}
 * to look up {@link SecretProviderFactory} from jar files and class loaders.
 */
public interface SecretProviderFactory {

  /**
   * Create a secret provider connected to a resource at a specific address. The address format
   * and semantics are implementation dependent.
   *
   * @param address The address of the resource the provider should connect to.
   * @return A provider.
   */
  SecretProvider create(String address);
}