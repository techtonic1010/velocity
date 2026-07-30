package com.velocity.recommendation.client;

import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.NeighborsResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class VectorHasherClientTest {

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsReturnsTheNeighborListOnSuccess() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N1")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class))
                .thenReturn(new NeighborsResponse("N1", List.of(new NeighborEntry("N2", 0.12))));

        VectorHasherClient client = new VectorHasherClient(restClient);
        Optional<List<NeighborEntry>> result = client.fetchNeighbors("N1");

        assertThat(result).contains(List.of(new NeighborEntry("N2", 0.12)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsReturnsEmptyOptionalOn404NotAnException() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N-unknown")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class)).thenThrow(
                HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders.EMPTY, new byte[0], null));

        VectorHasherClient client = new VectorHasherClient(restClient);

        assertThat(client.fetchNeighbors("N-unknown")).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsPropagatesOtherErrorsInsteadOfSwallowingThem() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N1")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class)).thenThrow(
                HttpClientErrorException.create(
                        HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY, new byte[0], null));

        VectorHasherClient client = new VectorHasherClient(restClient);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.fetchNeighbors("N1"))
                .isInstanceOf(HttpClientErrorException.class);
    }
}
