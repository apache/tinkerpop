# Gremlin Server

Gremlin Server lets you remote gremlin to places.

## Overview

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus consequat urna tortor, hendrerit dapibus elit bibendum vel. Nulla at vestibulum neque, vel pretium massa. Integer aliquam blandit erat. Aliquam sed nibh nec nisi fermentum luctus sed lobortis erat. Suspendisse ac augue eget diam euismod auctor non sed orci. Morbi dui ante, feugiat nec lacinia nec, ultrices sit amet purus. Vestibulum eget metus malesuada, dapibus felis non, malesuada diam. Morbi dui erat, iaculis sit amet risus in, sollicitudin lacinia magna. Vivamus sed dignissim purus, vitae rutrum metus. Ut condimentum quam purus, sed semper mi sodales ut.

Integer ultricies tempus arcu. Nunc quis vestibulum nulla, vitae laoreet nisi. Suspendisse magna velit, ullamcorper vitae justo quis, mattis pharetra turpis. Ut eu condimentum justo, in facilisis ipsum. Nunc tortor urna, vestibulum eget purus ut, rutrum feugiat tellus. Interdum et malesuada fames ac ante ipsum primis in faucibus. Aenean laoreet elit nibh, nec commodo eros feugiat ut. Ut sit amet nisl sem. Cras venenatis mollis mattis. Pellentesque velit magna, pulvinar id aliquet ut, lobortis vitae sapien. Sed dignissim tincidunt blandit. Duis rhoncus convallis sapien, tristique ornare urna venenatis vel.

## Getting Started

Aliquam non augue mattis, pellentesque enim a, aliquam arcu. Sed nibh erat, hendrerit non nulla eget, scelerisque gravida turpis. Pellentesque euismod bibendum massa nec ultrices. Mauris blandit consectetur erat vitae mollis. Integer pharetra luctus commodo. Sed lobortis est et odio tristique, in vulputate arcu pellentesque. Pellentesque vestibulum blandit mauris vitae malesuada. Sed blandit euismod augue, ac blandit nulla faucibus blandit.

<pre><code class="no-highlight">
gremlin> x = [1,2,3]
==>1
==>2
==>3
gremlin> x._().transform{it+1}
==>2
==>3
==>4
gremlin> x = g.E.has('weight', T.gt, 0.5f).toList()
==>e[10][4-created->5]
==>e[8][1-knows->4]
gremlin> x.inV
==>[StartPipe, InPipe]
==>[StartPipe, InPipe]
gremlin> x._().inV
==>v[5]
==>v[4]
</code></pre>

Here's some java:

<pre><code class="groovy">
final String s = "This is a test";
System.out.println(s);
</code></pre>

How about some groovy now:

<pre><code class="groovy">
def x = [1,2,3]
x._().transform{it+1}
x = g.E.has('weight', T.gt, 0.5f).toList()
</code></pre>

Etiam metus urna, pellentesque nec ligula vitae, laoreet varius ligula. Ut et elementum lorem. Pellentesque accumsan massa felis, quis ultrices ante interdum venenatis. Integer semper posuere sapien. Fusce non posuere felis. Etiam nec mattis turpis. Quisque venenatis tempor diam, non sollicitudin nisi. Nunc at lacus quis mi tempor viverra. Curabitur ut enim elit. Donec sit amet libero et urna rhoncus molestie sed sit amet turpis. Curabitur adipiscing metus pretium quam fermentum auctor. Nam tincidunt sollicitudin arcu quis hendrerit.

Vivamus a arcu risus. Etiam quis tellus et nibh rutrum volutpat ut nec nisi. Phasellus dignissim lobortis ligula nec venenatis. Phasellus vitae lorem vitae turpis consequat auctor. Pellentesque dui velit, commodo mollis laoreet vel, rutrum quis ligula. Cras imperdiet, massa ac vestibulum pellentesque, massa felis mattis odio, vel viverra purus erat ac tellus. Curabitur ut tempor elit. Nunc nulla risus, ullamcorper quis euismod ut, fringilla vitae ligula.
