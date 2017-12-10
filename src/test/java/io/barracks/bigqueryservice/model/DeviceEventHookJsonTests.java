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

package io.barracks.bigqueryservice.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.barracks.bigqueryservice.utils.DeviceRequestUtils;
import io.barracks.bigqueryservice.utils.GoogleClientSecretUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@JsonTest
public class DeviceEventHookJsonTests {

    @Autowired
    private JacksonTester<DeviceRequest> json;
    @Autowired
    private ObjectMapper objectMapper;
    @Value("classpath:io/barracks/bigqueryservice/model/deviceEventHook.json")
    private Resource deviceRequestResource;

    @Test
    public void deserializeJson_shouldDeserialize() throws Exception {
        // Given
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode root = mapper.createObjectNode();

        final Package package1 = Package.builder().reference("Reference of the first package").version("Version for the first package").build();
        final Package package2 = Package.builder().reference("Reference of the second package").version("Version for the second package").build();

        final GoogleClientSecret googleClientSecret = GoogleClientSecretUtils.getGoogleClientSecret().toBuilder()
                .type("service_account")
                .projectId("ID of the project")
                .privateKeyId("ID of the user's private key")
                .privateKey("User's google private key")
                .clientEmail("Email of the user")
                .clientId("User's ID")
                .authUri("https://accounts.google.com/o/oauth2/auth")
                .tokenUri("https://accounts.google.com/o/oauth2/token")
                .authProviderCertUrl("https://www.googleapis.com/oauth2/v1/certs")
                .clientCertUrl("https://www.googleapis.com/robot/v1/metadata/x509/105693303195-compute%40developer.gserviceaccount.com")
                .build();

        final DeviceRequest deviceRequest = DeviceRequestUtils.getDeviceRequest().toBuilder()
                .userId("Unique ID for the user")
                .customClientData(root)
                .packages(Arrays.asList(package1, package2))
                .unitId("ID transmitted by the device")
                .ipAddress("IP address of the device")
                .userAgent("Version of the SDK installed on the device that sent the information")
                .build();

        final DeviceEvent deviceEvent = DeviceEvent.builder()
                .request(deviceRequest)
                .response(
                        ResolvedVersions.builder().build()
                )
                .build();

        final Hook hook = Hook.builder().googleClientSecret(googleClientSecret).name("The name of this hook").build();

        final DeviceEventHook expected = DeviceEventHook.builder()
                .deviceEvent(deviceEvent)
                .hook(hook)
                .build();

        // When
        final DeviceEventHook result = objectMapper.readValue(deviceRequestResource.getInputStream(), DeviceEventHook.class);


        // Then
        assertThat(expected).hasNoNullFieldsOrProperties();
        assertThat(result).isEqualTo(expected);
    }

}
