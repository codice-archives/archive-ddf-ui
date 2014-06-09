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

import java.util.UUID;

import javax.xml.transform.TransformerException;

import org.codice.ddf.platform.cassandra.embedded.CassandraEmbeddedServer;
import org.codice.ddf.ui.searchui.query.model.SearchRequest;
import org.geotools.filter.FilterTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

import ddf.catalog.operation.Query;

public class SavedQueryOCM {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SavedQueryOCM.class);
    
    static final String KEYSPACE_NAME = "ddf_ui";
    
    static final String SAVED_QUERIES_TABLE_NAME = "saved_queries";
    
    private static final String SAVED_QUERIES_TABLE_SCHEMA = 
//            "id uuid PRIMARY KEY, userId text, createdDate timeuuid, filter text";
        "id uuid PRIMARY KEY, userId text, createdDate timeuuid";
    
    private CassandraEmbeddedServer cassandraServer;

    public SavedQueryOCM(CassandraEmbeddedServer cassandraServer) {
        this.cassandraServer = cassandraServer;
        this.cassandraServer.createKeyspace(KEYSPACE_NAME);
        this.cassandraServer.createTable(KEYSPACE_NAME, SAVED_QUERIES_TABLE_NAME, SAVED_QUERIES_TABLE_SCHEMA);
    }
    
    public boolean store(SearchRequest request, String username) {
        Query query = request.getQuery();
        FilterTransformer transform = new FilterTransformer();
        transform.setIndentation(2);
        String filterXml = null;
        try {
            filterXml = transform.transform(query);
        } catch (TransformerException e) {
            LOGGER.error("Cannot convert query to filter XML", e);
            return false;
        }
        
        Session session = this.cassandraServer.getSession(KEYSPACE_NAME); 
        String normalizedFilterXml = filterXml.replaceAll("\n",  "").replaceAll("'", "''");
        
        String insertCql = "INSERT INTO " + KEYSPACE_NAME + "." + SAVED_QUERIES_TABLE_NAME + "(id, userId, createdDate, filter) VALUES (" +
            UUID.randomUUID().toString() + ", '" + username + "', now(), '" + normalizedFilterXml + "')";
        LOGGER.info("insertCql = {}", insertCql);
        session.execute(insertCql);
        return true;
    }
}
