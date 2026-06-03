import '../lib/driver/connection.dart';
import '../lib/driver/request_message.dart';

void main() async {
  // Sack with float32 (f) vs double (D)
  await _test('sack with 0.5f',  'g.withSack(2147483647i).inject(0.5f).sack(div).sack()', 'gmodern');
  await _test('sack with 0.5D',  'g.withSack(2147483647i).inject(0.5D).sack(div).sack()', 'gmodern');
  await _test('sack with 0.5',   'g.withSack(2147483647i).inject(0.5).sack(div).sack()', 'gmodern');

  // Conjoin: list vs separate args
  await _test('conjoin [null,null]', 'g.inject([null,null]).conjoin("+")', 'ggraph');
  await _test('conjoin null,null',   'g.inject(null, null).conjoin("+")', 'ggraph');
}

Future<void> _test(String name, String gremlin, String g) async {
  final req = RequestMessage.build(gremlin).addG(g).addBulkResults(true).create();
  final c = Connection('http://localhost:45940/gremlin');
  try {
    final rs = await c.submit(req);
    print('[$name] → ${rs.items}');
  } catch (e) {
    print('[$name] ERROR: $e');
  } finally {
    await c.close();
  }
}
