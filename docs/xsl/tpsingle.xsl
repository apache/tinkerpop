<!--
  Generates single XHTML document from DocBook XML source using DocBook XSL
  stylesheets.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
    xmlns:sbhl="java:net.sf.xslthl.ConnectorSaxonB"
    xmlns:xslthl="http://xslthl.sf.net"
    extension-element-prefixes="sbhl xslthl">

  <xsl:import href="http://docbook.sourceforge.net/release/xsl-ns/current/html/docbook.xsl"/>
  <xsl:import href="tphighlight.xsl"/>
  <xsl:import href="tpcommon.xsl"/>
  
  <xsl:output
      method="html"
      encoding="utf-8"
      doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"
      doctype-system="http://www.w3.org/TR/html4/loose.dtd"
      indent="yes" />
  
  <xsl:param name="generate.toc">
  book         toc,title
  book/part    title
  book/chapter title
  </xsl:param>
  
  <xsl:param name="highlight.xslthl.config">file://$MAVEN{xsl.output.dir}/xslthl-config.xml</xsl:param>

  <!-- Appears inside the HTML <head/> tag -->
  <xsl:template name="user.head.content">
    <link href="css/tp3.css" media="all" rel="stylesheet" type="text/css" />
  </xsl:template>

  <!-- Appears inside the HTML <body/> tag, just before page content -->
  <xsl:template name="user.header.content">
    <!-- Body header content -->
  </xsl:template>

  <!-- Appears inside the HTML <body/> tag, just after page content -->
  <xsl:template name="user.footer.content">
    <!-- Body footer content -->
  </xsl:template>

  <!-- The docbook.xsl template provides other extension points.
       Any <xsl:template/> tag in docbook.xsl can be overridden in
       this file, and almost any style customization can be effected
       by overridding templates or setting <xsl:param/> values.

       http://docbook.sourceforge.net/release/xsl-ns/current/html/docbook.xsl
       -->

</xsl:stylesheet>
