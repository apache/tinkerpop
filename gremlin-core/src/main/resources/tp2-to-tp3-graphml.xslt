<?xml version="1.0" ?>
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
        </edge>
    </xsl:template>

</xsl:stylesheet>
