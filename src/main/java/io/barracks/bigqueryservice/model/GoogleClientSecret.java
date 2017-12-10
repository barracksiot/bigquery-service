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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.hibernate.validator.constraints.NotBlank;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GoogleClientSecret {

    @NotBlank
    private final String type;

    @JsonProperty("project_id")
    @NotBlank
    private final String projectId;

    @JsonProperty("private_key_id")
    @NotBlank
    private final String privateKeyId;

    @JsonProperty("private_key")
    @NotBlank
    private final String privateKey;

    @JsonProperty("client_email")
    @NotBlank
    private final String clientEmail;

    @JsonProperty("client_id")
    @NotBlank
    private final String clientId;

    @JsonProperty("auth_uri")
    @NotBlank
    private final String authUri;

    @JsonProperty("token_uri")
    @NotBlank
    private final String tokenUri;

    @JsonProperty("auth_provider_x509_cert_url")
    private final String authProviderCertUrl;

    @JsonProperty("client_x509_cert_url")
    private final String clientCertUrl;

    @JsonCreator
    public static GoogleClientSecret fromJson() {
        return builder().build();
    }

}
