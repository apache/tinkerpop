<?xml version="1.0" ?>
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->


<!-- XSL stylesheet to convert TinkerPop v2 GraphML files for Apache TinkerPop v3 -->
<xsl:stylesheet version="1.0"
                xmlns="http://graphml.graphdrawing.org/xmlns"
                xmlns:graphml="http://graphml.graphdrawing.org/xmlns"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                exclude-result-prefixes="graphml">
    <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>
    <xsl:strip-space elements="*"/>

    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="graphml:graphml">
        <graphml>
            <key id="labelV" for="node" attr.name="labelV" attr.type="string"/>
            <key id="labelE" for="edge" attr.name="labelE" attr.type="string"/>
            <xsl:apply-templates/>
        </graphml>
    </xsl:template>

    <xsl:template match="graphml:node">
        <node>
            <xsl:apply-templates select="node()|@*"/>
            <data key="labelV">vertex</data>
        </node>
    </xsl:template>

    <xsl:template match="graphml:edge">
        <edge id="{@id}" source="{@source}" target="{@target}">
            <data key="labelE">
                <xsl:value-of select="@label"/>
            </data>
            <xsl:apply-templates select="node()|@*"/>
        </edge>
    </xsl:template>

</xsl:stylesheet>
