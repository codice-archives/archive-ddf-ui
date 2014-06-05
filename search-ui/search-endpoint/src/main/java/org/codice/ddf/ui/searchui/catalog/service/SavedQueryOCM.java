/**
 * Copyright (c) Codice Foundation
 * 
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 * 
 **/
package org.codice.ddf.ui.searchui.catalog.service;

import org.codice.ddf.platform.cassandra.embedded.CassandraEmbeddedServer;
import org.codice.ddf.ui.searchui.query.model.SearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SavedQueryOCM {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SavedQueryOCM.class);
    
    private CassandraEmbeddedServer cassandraServer;

    public SavedQueryOCM(CassandraEmbeddedServer cassandraServer) {
        this.cassandraServer = cassandraServer;
    }
    
    public boolean store(SearchRequest request, String username) {
        return false;
    }
}
