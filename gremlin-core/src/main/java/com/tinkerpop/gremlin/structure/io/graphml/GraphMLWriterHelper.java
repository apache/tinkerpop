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

        @Override
        public void writeStartElement(final String localName) throws XMLStreamException {
            writer.writeStartElement(localName);
        }

        @Override
        public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
            writer.writeStartElement(namespaceURI, localName);
        }

        @Override
        public void writeStartElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            writer.writeStartElement(prefix, localName, namespaceURI);
        }

        @Override
        public void writeEmptyElement(final String namespaceURI, final String localName) throws XMLStreamException {
            writer.writeEmptyElement(namespaceURI, localName);
        }

        @Override
        public void writeEmptyElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            writer.writeEmptyElement(prefix, localName, namespaceURI);
        }

        @Override
        public void writeEmptyElement(final String localName) throws XMLStreamException {
            writer.writeEmptyElement(localName);
        }

        @Override
        public void writeEndElement() throws XMLStreamException {
            writer.writeEndElement();
        }

        @Override
        public void writeEndDocument() throws XMLStreamException {
            writer.writeEndDocument();
        }

        @Override
        public void close() throws XMLStreamException {
            writer.close();
        }

        @Override
        public void flush() throws XMLStreamException {
            writer.flush();
        }

        @Override
        public void writeAttribute(final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(localName, value);
        }

        @Override
        public void writeAttribute(final String prefix, final String namespaceURI, final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(prefix, namespaceURI, localName, value);
        }

        @Override
        public void writeAttribute(final String namespaceURI, final String localName, final String value) throws XMLStreamException {
            writer.writeAttribute(namespaceURI, localName, value);
        }

        @Override
        public void writeNamespace(final String prefix, final String namespaceURI) throws XMLStreamException {
            writer.writeNamespace(prefix, namespaceURI);
        }

        @Override
        public void writeDefaultNamespace(final String namespaceURI) throws XMLStreamException {
            writer.writeDefaultNamespace(namespaceURI);
        }

        @Override
        public void writeComment(final String data) throws XMLStreamException {
            writer.writeComment(data);
        }

        @Override
        public void writeProcessingInstruction(final String target) throws XMLStreamException {
            writer.writeProcessingInstruction(target);
        }

        @Override
        public void writeProcessingInstruction(final String target, final String data) throws XMLStreamException {
            writer.writeProcessingInstruction(target, data);
        }

        @Override
        public void writeCData(final String data) throws XMLStreamException {
            writer.writeCData(data);
        }

        @Override
        public void writeDTD(final String dtd) throws XMLStreamException {
            writer.writeDTD(dtd);
        }

        @Override
        public void writeEntityRef(final String name) throws XMLStreamException {
            writer.writeEntityRef(name);
        }

        @Override
        public void writeStartDocument() throws XMLStreamException {
            writer.writeStartDocument();
        }

        @Override
        public void writeStartDocument(final String version) throws XMLStreamException {
            writer.writeStartDocument(version);
        }

        @Override
        public void writeStartDocument(final String encoding, final String version) throws XMLStreamException {
            writer.writeStartDocument(encoding, version);
        }

        @Override
        public void writeCharacters(final String text) throws XMLStreamException {
            writer.writeCharacters(text);
        }

        @Override
        public void writeCharacters(final char[] text, final int start, final int len) throws XMLStreamException {
            writer.writeCharacters(text, start, len);
        }

        @Override
        public String getPrefix(final String uri) throws XMLStreamException {
            return writer.getPrefix(uri);
        }

        @Override
        public void setPrefix(final String prefix, final String uri) throws XMLStreamException {
            writer.setPrefix(prefix, uri);
        }

        @Override
        public void setDefaultNamespace(final String uri) throws XMLStreamException {
            writer.setDefaultNamespace(uri);
        }

        @Override
        public void setNamespaceContext(final NamespaceContext context) throws XMLStreamException {
            writer.setNamespaceContext(context);
        }

        @Override
        public NamespaceContext getNamespaceContext() {
            return writer.getNamespaceContext();
        }

        @Override
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


        @Override
        public void writeStartDocument() throws XMLStreamException {
            super.writeStartDocument();
            super.writeCharacters(lineSeparator);
        }

        @Override
        public void writeStartDocument(final String version) throws XMLStreamException {
            super.writeStartDocument(version);
            super.writeCharacters(lineSeparator);
        }

        @Override
        public void writeStartDocument(final String encoding, final String version) throws XMLStreamException {
            super.writeStartDocument(encoding, version);
            super.writeCharacters(lineSeparator);
        }

        @Override
        public void writeStartElement(final String localName) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(localName);
        }

        @Override
        public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(namespaceURI, localName);
        }

        @Override
        public void writeStartElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            onStartElement();
            super.writeStartElement(prefix, localName, namespaceURI);
        }

        @Override
        public void writeEmptyElement(final String namespaceURI, final String localName) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(namespaceURI, localName);
        }

        @Override
        public void writeEmptyElement(final String prefix, final String localName, final String namespaceURI) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(prefix, localName, namespaceURI);
        }

        @Override
        public void writeEmptyElement(final String localName) throws XMLStreamException {
            onEmptyElement();
            super.writeEmptyElement(localName);
        }

        @Override
        public void writeEndElement() throws XMLStreamException {
            onEndElement();
            super.writeEndElement();
        }

        @Override
        public void writeCharacters(final String text) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCharacters(text);
        }

        @Override
        public void writeCharacters(final char[] text, final int start, final int len) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCharacters(text, start, len);
        }

        @Override
        public void writeCData(final String data) throws XMLStreamException {
            state = SEEN_DATA;
            super.writeCData(data);
        }
    }
}

