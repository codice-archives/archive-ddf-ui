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
package org.codice.ddf.ui.searchui.query.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.server.BayeuxServer;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.server.AbstractService;

/**
 * Created by tustisos on 12/10/13.
 */
public class SearchResultService extends AbstractService {

    Map<String, ServerSession> channelToClientMap = new HashMap<String, ServerSession>();

    public SearchResultService(BayeuxServer bayeux, String name) {
        super(bayeux, name);
        addService("/service/results/subscribe", "processSubscribe");
        addService("/service/results/unsubscribe", "processUnsubscribe");
    }

    public void processSubscribe(final ServerSession remote, Message message) {
        Map<String, Object> input = message.getDataAsMap();
        String channel = (String) input.get("channel");

        getBayeux().createChannelIfAbsent(channel);

        channelToClientMap.put(channel, remote);

        remote.deliver(getServerSession(), channel, "Subscribed", null);
    }

    public void processUnsubscribe(final ServerSession remote, Message message) {
        Map<String, Object> input = message.getDataAsMap();
        String channel = (String) input.get("channel");

        removeService(channel);

        channelToClientMap.remove(channel);

        remote.deliver(getServerSession(), channel, "Unsubscribed", null);
    }

    public synchronized void pushResults(UUID channel, String jsonData) {
        String channelName = "/"+channel.toString();

        getBayeux().createChannelIfAbsent(channelName);
        // getBayeux().getChannel(channelName).publish(getServerSession(), jsonData, null);
        waitForChannel(channelName);
        if (channelToClientMap.containsKey(channelName)) {
            send(channelToClientMap.get(channelName), channelName, jsonData, null);
        }
    }

    public synchronized void pushResults(String channel, String jsonData) {
        String channelName = "/" + channel.toString();

        getBayeux().createChannelIfAbsent(channelName);
        // getBayeux().getChannel(channelName).publish(getServerSession(), jsonData, null);
        waitForChannel(channelName);
        if (channelToClientMap.containsKey(channelName)) {
            send(channelToClientMap.get(channelName), channelName, jsonData, null);
      }
    }

    private void waitForChannel(String channelName) {
        int timeout = 0;
        while (!channelToClientMap.containsKey(channelName) && timeout < 120) {
            timeout++;
            try {
                TimeUnit.MICROSECONDS.sleep(500);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}