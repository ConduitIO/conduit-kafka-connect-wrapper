/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CombinedSchemaProviderTest {
    private CombinedSchemaProvider underTest;
    @Mock
    private RawDataSchemaProvider rawSP;
    @Mock
    private StructSchemaProvider structSP;

    @BeforeEach
    public void setUp() {
        underTest = new CombinedSchemaProvider(rawSP, structSP);
    }

    @Test
    public void testNullRecord() {
        assertNull(underTest.provide(null));
    }

    @Test
    public void testNoPayload() {
        assertNull(underTest.provide(Record.newBuilder().build()));
    }

    @Test
    public void testNoAfter() {
        Record rec = Record.newBuilder()
            .setPayload(Change.newBuilder().build())
            .build();
        assertNull(underTest.provide(rec));
    }

    @Test
    public void testRaw() {
        Change payload = Change.newBuilder()
            .setAfter(Data.newBuilder().setRawData(ByteString.copyFromUtf8("raw data")).build())
            .build();
        Record rec = Record.newBuilder()
            .setPayload(payload)
            .build();

        Schema expected = mock(Schema.class);
        when(rawSP.provide(rec)).thenReturn(expected);

        Schema actual = underTest.provide(rec);
        verify(rawSP).provide(rec);
        assertEquals(expected, actual);
    }

    @Test
    public void testStruct() {
        Struct struct = Struct.newBuilder().build();
        Change payload = Change.newBuilder()
            .setAfter(Data.newBuilder().setStructuredData(struct).build())
            .build();
        Record rec = Record.newBuilder()
            .setPayload(payload)
            .build();

        Schema expected = mock(Schema.class);
        when(structSP.provide(rec)).thenReturn(expected);

        Schema actual = underTest.provide(rec);
        verify(structSP).provide(rec);
        assertEquals(expected, actual);
    }
}
