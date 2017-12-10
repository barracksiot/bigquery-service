/*
 * MIT License
 *
 * Copyright (c) 2017 Barracks Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.barracks.bigqueryservice.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMq;
import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMqConfig;
import io.barracks.bigqueryservice.Application;
import io.barracks.bigqueryservice.config.RabbitMQConfig;
import io.barracks.bigqueryservice.manager.BigQueryManager;
import io.barracks.bigqueryservice.model.DeviceChangeEventHook;
import io.barracks.bigqueryservice.model.DeviceEventHook;
import io.barracks.bigqueryservice.model.DeviceRequest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RabbitMQConfig.class, Application.class})
public class DeviceMessageReceiverTest {

    private static EmbeddedRabbitMq rabbitMq;

    @SpyBean
    private DeviceMessageReceiver receiver;
    @Value("${io.barracks.amqp.exchangename}")
    private String exchangeName;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @MockBean
    private BigQueryManager bigQueryManager;
    @Autowired
    private ObjectMapper objectMapper;
    @Value("classpath:io/barracks/bigqueryservice/rabbitmq/deviceEventHook.json")
    private Resource resource;
    @Value("classpath:io/barracks/bigqueryservice/rabbitmq/deviceChangeEventHook.json")
    private Resource changeResource;

    @BeforeClass
    public static void startBroker() {
        EmbeddedRabbitMqConfig config = new EmbeddedRabbitMqConfig.Builder()
                .erlangCheckTimeoutInMillis(150000)
                .rabbitMqServerInitializationTimeoutInMillis(100000)
                .defaultRabbitMqCtlTimeoutInMillis(100000)
                .downloadConnectionTimeoutInMillis(150000)
                .downloadReadTimeoutInMillis(150000)
                .build();
        rabbitMq = new EmbeddedRabbitMq(config);
        rabbitMq.start();
    }

    @AfterClass
    public static void clear() throws Exception {
        rabbitMq.stop();
    }

    @Before
    public void isBrokerRunning() {
        BrokerRunning.isRunning();
    }

    @Test
    public void receiveMessage_whenAllIsFine_shouldCallManager() throws Exception {
        //Given
        final DeviceEventHook message = objectMapper.readValue(resource.getInputStream(), DeviceEventHook.class);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MediaType.APPLICATION_JSON_VALUE);

        //When
        this.rabbitTemplate.convertSendAndReceive(exchangeName, "test.v2.afsdsf", new Message(FileCopyUtils.copyToByteArray(resource.getInputStream()), messageProperties));

        //Then
        verify(receiver).receiveMessage(message);
        verify(bigQueryManager).sendEventToBigQuery(message);
    }

    @Test
    public void receiveChangeMessage_whenAllIsFine_shouldCallManager() throws Exception {
        //Given
        final DeviceChangeEventHook message = objectMapper.readValue(changeResource.getInputStream(), DeviceChangeEventHook.class);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MediaType.APPLICATION_JSON_VALUE);

        //When
        this.rabbitTemplate.convertSendAndReceive(exchangeName, "devices.change.v2.afsdsf", new Message(FileCopyUtils.copyToByteArray(changeResource.getInputStream()), messageProperties));

        //Then
        verify(receiver).receiveMessage(message);
        verify(bigQueryManager).sendEventToBigQuery(message);
    }

}
