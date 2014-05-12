<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <!-- Navigation and section labels -->
  <xsl:param name="suppress.navigation" select="0"/>
  <xsl:param name="navig.showtitles">1</xsl:param>
  <xsl:param name="section.autolabel.max.depth" select="5"/>
  <xsl:param name="section.autolabel" select="1"/>
  <xsl:param name="section.label.includes.component.label" select="1"/>

  <!-- Chunking -->
  <xsl:param name="chunk.first.sections" select="1"/>
  <xsl:param name="chunk.section.depth" select="0"/>
  <xsl:param name="chunk.toc" select="''"/>
  <xsl:param name="chunk.tocs.and.lots" select="0"/>

  <!-- Code syntax highlighting -->
  <xsl:param name="highlight.source" select="1"/>

  <!-- Table configuration -->
  <xsl:param name="table.borders.with.css" select="1"/>
  <!-- <xsl:param name="table.cell.border.color" select="'#747474'"/> -->
  <xsl:param name="table.cell.border.style" select="'solid'"/>
  <xsl:param name="table.cell.border.thickness" select="'1px'"/>
  <!-- <xsl:param name="table.frame.border.color" select="'#FF8080'"/> -->
  <xsl:param name="table.frame.border.style" select="'solid'"/>
  <xsl:param name="table.frame.border.thickness" select="'1px'"/>
  <xsl:param name="tablecolumns.extension" select="'1'"/>
  <xsl:param name="html.cellpadding" select="'4px'"/>
  <xsl:param name="html.cellspacing" select="''"/>

</xsl:stylesheet>
