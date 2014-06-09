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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.TransformerException;

import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.session.mgt.SimpleSession;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.codice.ddf.platform.cassandra.embedded.CassandraConfig;
import org.codice.ddf.platform.cassandra.embedded.CassandraEmbeddedServer;
import org.codice.ddf.ui.searchui.query.controller.SearchController;
import org.geotools.filter.FilterTransformer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import ddf.catalog.CatalogFramework;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.SourceInfoResponse;
import ddf.catalog.operation.impl.SourceInfoRequestEnterprise;
import ddf.catalog.source.SourceDescriptor;

public class CatalogServiceTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceTest.class);

    @BeforeClass
    public static void oneTimeSetup() throws IOException {
//        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
//                .getLogger(Logger.ROOT_LOGGER_NAME);
//        root.setLevel(ch.qos.logback.classic.Level.DEBUG);
        String workingDir = System.getProperty("user.dir") + File.separator + "target";
        System.setProperty("karaf.home", workingDir);
    }
    
    @Test
    public void test() throws Exception {
        org.apache.shiro.mgt.SecurityManager secManager = new DefaultSecurityManager();
        PrincipalCollection principals = new SimplePrincipalCollection("user1", "testrealm");
        Subject subject = new Subject.Builder(secManager).principals(principals)
                .session(new SimpleSession()).authenticated(true).buildSubject();
        
        SourceDescriptor sourceDescriptor = mock(SourceDescriptor.class);
        when(sourceDescriptor.isAvailable()).thenReturn(true);
        when(sourceDescriptor.getSourceId()).thenReturn("source-1");
        Set<SourceDescriptor> sourceInfo = new HashSet<SourceDescriptor>();
        sourceInfo.add(sourceDescriptor);
        SourceInfoResponse sourceInfoResponse = mock(SourceInfoResponse.class);
        when(sourceInfoResponse.getSourceInfo()).thenReturn(sourceInfo);
        CatalogFramework framework = mock(CatalogFramework.class);
        when(framework.getSourceInfo(any(SourceInfoRequestEnterprise.class))).thenReturn(sourceInfoResponse);
        
        FilterBuilder filterBuilder = new GeotoolsFilterBuilder();
        SearchController searchController = mock(SearchController.class);
        when(searchController.getFramework()).thenReturn(framework);
        
        // Simulates what blueprint would do
        CassandraConfig cassandraConfig = new CassandraConfig("DDF Cluster", "conf/cassandra.yaml", 9160, 9042, 7000, 7001, 
                "/data", "/commitlog", "/saved_caches");
        CassandraEmbeddedServer cassandraServer = new CassandraEmbeddedServer(cassandraConfig);
        SavedQueryOCM savedQueryOcm = new SavedQueryOCM(cassandraServer);
        TableMetadata tableMetadata = cassandraServer.getTable(SavedQueryOCM.KEYSPACE_NAME, SavedQueryOCM.SAVED_QUERIES_TABLE_NAME);
        assertNotNull(tableMetadata);
        assertNotNull(tableMetadata.getColumn("id"));

        CatalogService catalogService = new CatalogService(filterBuilder, searchController, savedQueryOcm);
        
        Map<String, Object> queryMessage = new HashMap<String, Object>();
        queryMessage.put(CatalogService.GUID, "my-guid");
        queryMessage.put(CatalogService.PHRASE, "test phrase");
        catalogService.saveQuery(queryMessage, subject);
        //printFilter(catalogService.getQuery());
        Session session = cassandraServer.getSession(SavedQueryOCM.KEYSPACE_NAME);
        Row row = session.execute("SELECT COUNT(*) FROM " + SavedQueryOCM.SAVED_QUERIES_TABLE_NAME).one();
        LOGGER.info("num rows in saved_queries table = {}", row.getLong(0));
        ResultSet resultSet = session.execute("SELECT * FROM " + SavedQueryOCM.SAVED_QUERIES_TABLE_NAME);
        int count = 0;
        for (Row savedQuery : resultSet.all()) {
            LOGGER.info("savedQuery #{}", ++count);
            LOGGER.info("uuid = {}", savedQuery.getUUID("id").toString());
            LOGGER.info("userId = {}", savedQuery.getString("userId"));
            //LOGGER.info("createdDate = {}", savedQuery.???);
            LOGGER.info("filter = {}", savedQuery.getString("filter"));
        }
    }
   
    private void printFilter(Filter filter) throws TransformerException {
        FilterTransformer transform = new FilterTransformer();
        transform.setIndentation(2);
        LOGGER.info(transform.transform(filter));
    }
}
