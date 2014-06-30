package com.tinkerpop.gremlin.structure.io.graphml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Stack;

/**
 * A wrapper for the IndentingXMLStreamWriter class by Kohsuke Kawaguchi
 */
class GraphMLWriterHelper {
    /**
     * Delegating {@link javax.xml.stream.XMLStreamWriter}.
     *
     * @author Kohsuke Kawaguchi
     */
    private abstract static class DelegatingXMLStreamWriter implements XMLStreamWriter {
        private final XMLStreamWriter writer;

        public DelegatingXMLStreamWriter(final XMLStreamWriter writer) {
            this.writer = writer;
        }

        public void writeStartElement(final String localName) throws XMLStreamException {
            writer.writeStartElement(localName);
        }

        public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
            writer.writeStartElement(namespaceURI, localName);
        }

        public void writeStartElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            writer.writeStartElement(prefix, localName, namespaceURI);
        }

        public void writeEmptyElement(final String namespaceURI, final String localName) throws XMLStreamException {
            writer.writeEmptyElement(namespaceURI, localName);
        }

        public void writeEmptyElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            writer.writeEmptyElement(prefix, localName, namespaceURI);
        }

        public void writeEmptyElement(final String localName) throws XMLStreamException {
            writer.writeEmptyElement(localName);
        }

        public void writeEndElement() throws XMLStreamException {
            writer.writeEndElement();
        }

        public void writeEndDocument() throws XMLStreamException {
            writer.writeEndDocument();
        }

        public void close() throws XMLStreamException {
            writer.close();
        }

        public void flush() throws XMLStreamException {
            writer.flush();
        }

        public void writeAttribute(final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(localName, value);
        }

        public void writeAttribute(final String prefix, final String namespaceURI, final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(prefix, namespaceURI, localName, value);
        }

        public void writeAttribute(final String namespaceURI, final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(namespaceURI, localName, value);
        }

        public void writeNamespace(final String prefix, final String namespaceURI) throws XMLStreamException {
            writer.writeNamespace(prefix, namespaceURI);
        }

        public void writeDefaultNamespace(final String namespaceURI) throws XMLStreamException {
            writer.writeDefaultNamespace(namespaceURI);
        }

        public void writeComment(final String data) throws XMLStreamException {
            writer.writeComment(data);
        }

        public void writeProcessingInstruction(final String target) throws XMLStreamException {
            writer.writeProcessingInstruction(target);
        }

        public void writeProcessingInstruction(final String target, final String data) throws XMLStreamException {
            writer.writeProcessingInstruction(target, data);
        }

        public void writeCData(final String data) throws XMLStreamException {
            writer.writeCData(data);
        }

        public void writeDTD(final String dtd) throws XMLStreamException {
            writer.writeDTD(dtd);
        }

        public void writeEntityRef(final String name) throws XMLStreamException {
            writer.writeEntityRef(name);
        }

        public void writeStartDocument() throws XMLStreamException {
            writer.writeStartDocument();
        }

        public void writeStartDocument(final String version) throws XMLStreamException {
            writer.writeStartDocument(version);
        }

        public void writeStartDocument(final String encoding, final String version) throws XMLStreamException {
            writer.writeStartDocument(encoding, version);
        }

        public void writeCharacters(final String text) throws XMLStreamException {
            writer.writeCharacters(text);
        }

        public void writeCharacters(final char[] text, final int start, final int len) throws XMLStreamException {
            writer.writeCharacters(text, start, len);
        }

        public String getPrefix(final String uri) throws XMLStreamException {
            return writer.getPrefix(uri);
        }

        public void setPrefix(final String prefix, final String uri) throws XMLStreamException {
            writer.setPrefix(prefix, uri);
        }

        public void setDefaultNamespace(final String uri) throws XMLStreamException {
            writer.setDefaultNamespace(uri);
        }

        public void setNamespaceContext(final NamespaceContext context) throws XMLStreamException {
            writer.setNamespaceContext(context);
        }

        public NamespaceContext getNamespaceContext() {
            return writer.getNamespaceContext();
        }

        public Object getProperty(final String name) throws IllegalArgumentException {
            return writer.getProperty(name);
        }
    }

    /**
     * @author Kohsuke Kawaguchi
     */
    public static class IndentingXMLStreamWriter extends DelegatingXMLStreamWriter {
        private final static Object SEEN_NOTHING = new Object();
        private final static Object SEEN_ELEMENT = new Object();
        private final static Object SEEN_DATA = new Object();

        private Object state = SEEN_NOTHING;
        private Stack<Object> stateStack = new Stack<>();

        private String indentStep = "  ";
        private static final String lineSeparator = System.getProperty("line.separator", "\n");
        private int depth = 0;

        public IndentingXMLStreamWriter(final XMLStreamWriter writer) {
            super(writer);
        }

        /**
         * Return the current indent step.
         * <p/>
         * <p>Return the current indent step: each start tag will be
         * indented by this number of spaces times the number of
         * ancestors that the element has.</p>
         *
         * @return The number of spaces in each indentation step,
         * or 0 or less for no indentation.
         * @see #setIndentStep(int)
         * @deprecated Only return the length of the indent string.
         */
        public int getIndentStep() {
            return indentStep.length();
        }


        /**
         * Set the current indent step.
         *
         * @param indentStep The new indent step (0 or less for no
         *                   indentation).
         * @see #getIndentStep()
         * @deprecated Should use the version that takes string.
         */
        public void setIndentStep(int indentStep) {
            final StringBuilder s = new StringBuilder();
            for (; indentStep > 0; indentStep--) s.append(' ');
            setIndentStep(s.toString());
        }

        public void setIndentStep(final String s) {
            this.indentStep = s;
        }

        private void onStartElement() throws XMLStreamException {
            stateStack.push(SEEN_ELEMENT);
            state = SEEN_NOTHING;
            if (depth > 0) {
                super.writeCharacters(lineSeparator);
            }
            doIndent();
            depth++;
        }

        private void onEndElement() throws XMLStreamException {
            depth--;
            if (state == SEEN_ELEMENT) {
                super.writeCharacters(lineSeparator);
                doIndent();
            }
            state = stateStack.pop();
        }

        private void onEmptyElement() throws XMLStreamException {
            state = SEEN_ELEMENT;
            if (depth > 0) {
                super.writeCharacters(lineSeparator);
            }
            doIndent();
        }

        /**
         * Print indentation for the current level.
         *
         * @throws javax.xml.stream.XMLStreamException If there is an error writing the indentation characters, or if a filter
         *                                             further down the chain raises an exception.
         */
        private void doIndent() throws XMLStreamException {
            if (depth > 0) {
                for (int i = 0; i < depth; i++)
                    super.writeCharacters(indentStep);
            }
        }


        public void writeStartDocument() throws XMLStreamException {
            super.writeStartDocument();
            super.writeCharacters(lineSeparator);
        }

        public void writeStartDocument(final String version) throws XMLStreamException {
            super.writeStartDocument(version);
            super.writeCharacters(lineSeparator);
        }

        public void writeStartDocument(final String encoding, final String version) throws XMLStreamException {
            super.writeStartDocument(encoding, version);
            super.writeCharacters(lineSeparator);
        }

        public void writeStartElement(final String localName) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(localName);
        }

        public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(namespaceURI, localName);
        }

        public void writeStartElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(prefix, localName, namespaceURI);
        }

        public void writeEmptyElement(final String namespaceURI, final String localName) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(namespaceURI, localName);
        }

        public void writeEmptyElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(prefix, localName, namespaceURI);
        }

        public void writeEmptyElement(final String localName) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(localName);
        }

        public void writeEndElement() throws XMLStreamException {
            onEndElement();
            super.writeEndElement();
        }

        public void writeCharacters(final String text) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCharacters(text);
        }

        public void writeCharacters(final char[] text, final int start, final int len) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCharacters(text, start, len);
        }

        public void writeCData(final String data) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCData(data);
        }
    }
}

