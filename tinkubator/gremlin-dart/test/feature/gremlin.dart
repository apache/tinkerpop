// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License
// for the specific language governing permissions and limitations under the License.
// ignore_for_file: non_constant_identifier_names
// AUTO-GENERATED - do not edit. Run build/generate.groovy to regenerate.

import 'dart:convert';
import 'package:gremlin_dart/process/anonymous_traversal.dart';
import 'package:gremlin_dart/process/graph_traversal.dart';
import 'package:gremlin_dart/process/traversal.dart';
import 'package:gremlin_dart/process/traversal_strategy.dart';

import 'package:uuid/uuid_value.dart';


final Map<String, List<Function>> generatedTraversals = <String, List<Function>>{
  'g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().branch(Anon.label().is_('person').count()).option(xx1, Anon.values('age')).option(xx2, Anon.values('lang')).option(xx2, Anon.values('name')),
  ],
  'g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().branch(Anon.label().is_('person').count()).option(xx1, Anon.values('age')).option(xx2, Anon.values('lang')).option(xx2, Anon.values('name')).option(pick.any, Anon.label()),
  ],
  'g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').branch(Anon.values('age')).option(P.lt(GInt(30)), Anon.constant('young')).option(P.gt(GInt(30)), Anon.constant('old')).option(pick.none, Anon.constant('on the edge')),
  ],
  'g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX': <Function>[
    (GraphTraversalSource g) => g.V().branch(Anon.identity()).option(Anon.hasLabel('software'), Anon.in_('created').values('name').order().fold()).option(Anon.has('name', 'vadas'), Anon.values('age')).option(P.neq(GInt(123)), Anon.bothE().count()),
  ],
  'g_V_chooseXout_countX_optionX2L_nameX_optionX3L_ageX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().choose(Anon.out().count()).option(xx1, Anon.values('name')).option(xx2, Anon.values('age')),
  ],
  'g_V_chooseXout_countX_optionX2L_nameX_optionX3L_ageX_optionXnone_discardX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().choose(Anon.out().count()).option(xx1, Anon.values('name')).option(xx2, Anon.values('age')).option(pick.none, Anon.discard()),
  ],
  'g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX_identityX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.hasLabel('person').and_().out('created'), Anon.out('knows'), Anon.identity()).values('name'),
  ],
  'g_V_chooseXhasLabelXpersonX_and_outXcreatedX_outXknowsX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.hasLabel('person').and_().out('created'), Anon.out('knows')).values('name'),
  ],
  'g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.label()).option('blah', Anon.out('knows')).option('bleep', Anon.out('created')).option(pick.none, Anon.identity()).values('name'),
  ],
  'g_V_chooseXTlabelX_optionXperson__outXknowsX_nameX_optionXbleep_constantXbleepXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(t.label).option('person', Anon.out('knows').values('name')).option('bleep', Anon.constant('bleep')),
  ],
  'g_V_chooseXTlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(t.label).option('blah', Anon.out('knows')).option('bleep', Anon.out('created')).option(pick.none, Anon.identity()).values('name'),
  ],
  'g_V_chooseXTlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone_discardX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(t.label).option('blah', Anon.out('knows')).option('bleep', Anon.out('created')).option(pick.none, Anon.discard()).values('name'),
  ],
  'g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.out('knows').count().is_(P.gt(GInt(0))), Anon.out('knows')).values('name'),
  ],
  'g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('p1').choose(Anon.outE('knows'), Anon.out('knows')).as_('p2').select('p1', 'p2').by('name'),
  ],
  'g_V_hasLabelXpersonX_chooseXageX__optionX27L__constantXyoungXX_optionXnone__constantXoldXX_groupCount': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasLabel('person').choose(Anon.values('age')).option(xx1, Anon.constant('young')).option(pick.none, Anon.constant('old')).groupCount(),
  ],
  'g_injectX1X_chooseXisX1X__constantX10Xfold__foldX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject(GInt(1)).choose(Anon.is_(xx1), Anon.constant(GInt(10)).fold(), Anon.fold()),
  ],
  'g_injectX2X_chooseXisX1X__constantX10Xfold__foldX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject(GInt(2)).choose(Anon.is_(xx1), Anon.constant(GInt(10)).fold(), Anon.fold()),
  ],
  'g_V_chooseXhasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_constantXxXX_optionXbetweenX20_30X_constantXyXX_optionXnone_constantXzXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.constant('x')).option(P.between(GInt(20), GInt(30)), Anon.constant('y')).option(pick.none, Anon.constant('z')),
  ],
  'g_V_chooseXhasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_orXgtX34XX_constantXxXX_optionXgtX34X_constantXyXX_optionXnone_constantXzXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)).or_(P.gt(GInt(34))), Anon.constant('x')).option(P.gt(GInt(34)), Anon.constant('y')).option(pick.none, Anon.constant('z')),
  ],
  'g_V_hasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name')).option(pick.none, Anon.values('name')),
  ],
  'g_V_chooseXhasLabelXpersonX_localXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').local(Anon.choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name').fold()).option(pick.none, Anon.values('name').fold())),
  ],
  'g_V_chooseXhasLabelXpersonX_mapXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').map_(Anon.choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name').fold()).option(pick.none, Anon.values('name').fold())),
  ],
  'g_unionXV_VXhasLabelXpersonX_barrier_localXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V(), Anon.V()).hasLabel('person').barrier().local(Anon.choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name').fold()).option(pick.none, Anon.values('name').fold())),
  ],
  'g_unionXV_VXhasLabelXpersonX_barrier_mapXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V(), Anon.V()).hasLabel('person').barrier().map_(Anon.choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name').fold()).option(pick.none, Anon.values('name').fold())),
  ],
  'g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name')).option(pick.none, Anon.values('name')),
  ],
  'g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX_optionXunproductive_labelX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name')).option(pick.none, Anon.values('name')).option(pick.unproductive, Anon.label()),
  ],
  'g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX_optionXnone_identityX_optionXnone_failX_optionXunproductive_identityX_optionXunproductive_labelX_optionXnone_failX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name')).option(pick.none, Anon.values('name')).option(pick.none, Anon.identity()).option(pick.none, Anon.fail()).option(pick.unproductive, Anon.label()).option(pick.unproductive, Anon.identity()).option(pick.unproductive, Anon.fail()),
  ],
  'g_V_chooseXage_nameX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age'), Anon.values('name')),
  ],
  'g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_discardX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age')).option(P.between(GInt(26), GInt(30)), Anon.values('name')).option(pick.none, Anon.discard()),
  ],
  'g_V_chooseXnameX_optionXneqXyX_ageX_optionXnone_constantXxXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('name')).option(P.neq('y'), Anon.values('age')).option(pick.none, Anon.constant('x')),
  ],
  'g_V_hasLabelXpersonX_chooseXoutXcreatedX_count_isXeqX0XX__constantXdidnt_createX__constantXcreatedXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.out('created').count().is_(P.eq(GInt(0))), Anon.constant('didnt_create'), Anon.constant('created')),
  ],
  'g_V_hasLabelXpersonX_chooseXvaluesXageX_isXgtX30XX__valuesXageX__constantX30XX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('age').is_(P.gt(GInt(30))), Anon.values('age'), Anon.constant(GInt(30))),
  ],
  'g_V_hasLabelXpersonX_chooseXvaluesXageX_isXgtX29XX_and_valuesXageX_isXltX35XX__valuesXnameX__constantXotherXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('age').is_(P.gt(GInt(29))).and_().values('age').is_(P.lt(GInt(35))), Anon.values('name'), Anon.constant('other')),
  ],
  'g_V_chooseXhasXname_vadasX__valuesXnameX__valuesXageXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.has('name', 'vadas'), Anon.values('name'), Anon.values('age')),
  ],
  'g_V_hasLabelXpersonX_chooseXoutXcreatedX_countX_optionX0__constantXnoneXX_optionX1__constantXoneXX_optionX2__constantXmanyXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx0, dynamic xx2}) => g.V().hasLabel('person').choose(Anon.out('created').count()).option(xx0, Anon.constant('none')).option(xx1, Anon.constant('one')).option(xx2, Anon.constant('many')),
  ],
  'g_V_hasLabelXpersonX_chooseXlocalXoutXknowsX_countX__optionX0__constantXnoFriendsXX__optionXnone__constantXhasFriendsXXX': <Function>[
    (GraphTraversalSource g, {dynamic xx0}) => g.V().hasLabel('person').choose(Anon.local(Anon.out('knows').count())).option(xx0, Anon.constant('noFriends')).option(pick.none, Anon.constant('hasFriends')),
  ],
  'g_V_chooseXoutE_countX_optionX0__constantXnoneXX_optionXnone__constantXsomeXX': <Function>[
    (GraphTraversalSource g, {dynamic xx0}) => g.V().choose(Anon.outE().count()).option(xx0, Anon.constant('none')).option(pick.none, Anon.constant('some')),
  ],
  'g_V_chooseXlabelX_optionXperson__chooseXageX_optionXP_lt_30__constantXyoungXX_optionXP_gte_30__constantXoldXXX_optionXsoftware__constantXprogramXX_optionXnone__constantXunknownXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.label()).option('person', Anon.choose(Anon.values('age')).option(P.lt(GInt(30)), Anon.constant('young')).option(P.gte(GInt(30)), Anon.constant('old'))).option('software', Anon.constant('program')).option(pick.none, Anon.constant('unknown')),
  ],
  'g_V_chooseXhasXname_vadasX__valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.has('name', 'vadas'), Anon.values('name')),
  ],
  'g_V_hasLabelXpersonX_age_chooseXP_eqX29X_constantXmatchedX_constantXotherXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('age').choose(P.eq(GInt(29)), Anon.constant('matched'), Anon.constant('other')),
  ],
  'g_V_hasLabelXpersonX_age_chooseXP_eqX29X_constantXmatchedX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('age').choose(P.eq(GInt(29)), Anon.constant('matched')),
  ],
  'g_V_hasLabelXpersonX_chooseX_valuesXnameX_option1X_isXmarkoX_valuesXageXX_option2Xnone_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('name')).option(Anon.is_('marko'), Anon.values('age')).option(pick.none, Anon.values('name')),
  ],
  'g_V_hasLabelXpersonX_chooseX_valuesXnameX_option1X_PeqXmarkoX_valuesXageXX_option2Xnone_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').choose(Anon.values('name')).option(P.eq('marko'), Anon.values('age')).option(pick.none, Anon.values('name')),
  ],
  'g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.properties('location').order().by(t.value_, order.asc).range(GInt(0), GInt(2))).value_(),
  ],
  'g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX': <Function>[
    (GraphTraversalSource g) => g.V().has(t.label, 'person').as_('a').local(Anon.out('created').as_('b')).select('a', 'b').by('name').by(t.id),
  ],
  'g_V_localXoutE_countX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.outE().count()),
  ],
  'g_VX1X_localXoutEXknowsX_limitX1XX_inV_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).local(Anon.outE('knows').limit(GInt(1))).inV().values('name'),
  ],
  'g_V_localXbothEXcreatedX_limitX1XX_otherV_name': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.bothE('created').limit(GInt(1))).otherV().values('name'),
  ],
  'g_VX4X_localXbothEX1_createdX_limitX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).local(Anon.bothE('created').limit(GInt(1))),
  ],
  'g_VX4X_localXbothEXknows_createdX_limitX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).local(Anon.bothE('knows', 'created').limit(GInt(1))),
  ],
  'g_VX4X_localXbothE_limitX1XX_otherV_name': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).local(Anon.bothE().limit(GInt(1))).otherV().values('name'),
  ],
  'g_VX4X_localXbothE_limitX2XX_otherV_name': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).local(Anon.bothE().limit(GInt(2))).otherV().values('name'),
  ],
  'g_V_localXinEXknowsX_limitX2XX_outV_name': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.inE('knows').limit(GInt(2))).outV().values('name'),
  ],
  'g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.match_(Anon.as_('project').in_('created').as_('person'), Anon.as_('person').values('name').as_('name'))).select('name', 'project').by().by('name'),
  ],
  'g_V_in_barrier_localXcountX': <Function>[
    (GraphTraversalSource g) => g.V().in_().barrier().local(Anon.count()),
  ],
  'g_V_localXout_in_simplePathX_path': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.out().in_().simplePath()).path(),
  ],
  'g_withSackX0LX_V_in_barrier_localXsackXsumX_byXageXX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GLong(0)).V().in_().barrier().local(Anon.sack(operator_.sum).by('age')).sack(),
  ],
  'g_V_localXout_localXcountXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.out().local(Anon.count())),
  ],
  'g_V_unionXoutE_count_localXinE_countXX': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.outE().count(), Anon.local(Anon.inE().count())),
  ],
  'g_VX2X_optionalXoutXknowsXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).optional(Anon.out('knows')),
  ],
  'g_VX2X_optionalXinXknowsXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).optional(Anon.in_('knows')),
  ],
  'g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').optional(Anon.out('knows').optional(Anon.out('created'))).path(),
  ],
  'g_V_optionalXout_optionalXoutXX_path': <Function>[
    (GraphTraversalSource g) => g.V().optional(Anon.out().optional(Anon.out())).path(),
  ],
  'g_VX1X_optionalXaddVXdogXX_label': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).optional(Anon.addV('dog')).label(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(),
  ],
  'g_V_repeatXoutX_timesX2X_emit_path': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(2)).emit().path(),
  ],
  'g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(2)).repeat(Anon.in_()).times(GInt(2)).values('name'),
  ],
  'g_V_repeatXoutE_inVX_timesX2X_path_by_name_by_label': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.outE().inV()).times(GInt(2)).path().by('name').by(t.label),
  ],
  'g_V_repeatXoutX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(2)),
  ],
  'g_V_repeatXoutX_timesX2X_emit': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(2)).emit(),
  ],
  'g_VX1X_timesX2X_repeatXoutX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).times(GInt(2)).repeat(Anon.out()).values('name'),
  ],
  'g_V_emit_timesX2X_repeatXoutX_path': <Function>[
    (GraphTraversalSource g) => g.V().emit().times(GInt(2)).repeat(Anon.out()).path(),
  ],
  'g_V_emit_repeatXoutX_timesX2X_path': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat(Anon.out()).times(GInt(2)).path(),
  ],
  'g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).emit(Anon.has(t.label, 'person')).repeat(Anon.out()).values('name'),
  ],
  'g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.groupCount('m').by('name').out()).times(GInt(2)).cap('m'),
  ],
  'g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.groupCount('m').by(Anon.loops()).out()).times(GInt(3)).cap('m'),
  ],
  'g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both()).times(GInt(10)).as_('a').out().as_('b').select('a', 'b').count(),
  ],
  'g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.out()).until(Anon.outE().count().is_(GInt(0))).values('name'),
  ],
  'g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').repeat(Anon.outE().inV().simplePath()).until(Anon.has('name', 'ripple')).path().by('name').by(t.label),
  ],
  'g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name': <Function>[
    (GraphTraversalSource g) => g.V().has('loops', 'name', 'loop').repeat(Anon.in_()).times(GInt(5)).path().by('name'),
  ],
  'g_V_repeatXout_repeatXout_order_byXname_descXX_timesX1XX_timesX1X_limitX1X_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out().repeat(Anon.out().order().by('name', order.desc)).times(GInt(1))).times(GInt(1)).limit(GInt(1)).path().by('name'),
  ],
  'g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out('knows')).until(Anon.repeat(Anon.out('created')).emit(Anon.has('name', 'lop'))).path().by('name'),
  ],
  'g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.repeat(Anon.out('created')).until(Anon.has('name', 'ripple'))).emit().values('lang'),
  ],
  'g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang': <Function>[
    (GraphTraversalSource g) => g.V().until(Anon.constant(true)).repeat(Anon.repeat(Anon.out('created')).until(Anon.has('name', 'ripple'))).emit().values('lang'),
  ],
  'g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat('a', Anon.out('knows').filter_(Anon.loops('a').is_(GInt(0)))).values('lang'),
  ],
  'g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.both('created')).until(Anon.loops().is_(GInt(40))).emit(Anon.repeat(Anon.in_('knows')).emit(Anon.loops().is_(GInt(1)))).dedup().values('name'),
  ],
  'g_VX1X_repeatXrepeatXunionXout_uses_out_traversesXX_whereXloops_isX0X_timesX1X_timeX2X_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.repeat(Anon.union(Anon.out('uses'), Anon.out('traverses')).where(Anon.loops().is_(GInt(0)))).times(GInt(1))).times(GInt(2)).values('name'),
  ],
  'g_V_repeatXa_outXknows_repeatXb_outXcreatedX_filterXloops_isX0XX_emit_lang': <Function>[
    (GraphTraversalSource g) => g.V().repeat('a', Anon.out('knows').repeat('b', Anon.out('created').filter_(Anon.loops('a').is_(GInt(0)))).emit()).emit().values('lang'),
  ],
  'g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name': <Function>[
    (GraphTraversalSource g, {dynamic vid6}) => g.V(vid6).repeat('a', Anon.both('created').simplePath()).emit(Anon.repeat('b', Anon.both('knows')).until(Anon.loops('b').as_('b').where(Anon.loops('a').as_('b'))).has('name', 'vadas')).dedup().values('name'),
  ],
  'g_V_emit': <Function>[
    (GraphTraversalSource g) => g.V().emit(),
  ],
  'g_V_untilXidentityX': <Function>[
    (GraphTraversalSource g) => g.V().until(Anon.identity()),
  ],
  'g_V_timesX5X': <Function>[
    (GraphTraversalSource g) => g.V().times(GInt(5)),
  ],
  'g_V_hasXperson_name_markoX_repeatXoutXcreatedXX_timesX1X_name': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').repeat(Anon.out('created')).times(GInt(1)).values('name'),
  ],
  'g_V_hasXperson_name_markoX_repeatXoutXcreatedXX_timesX0X_name': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').repeat(Anon.out('created')).times(GInt(0)).values('name'),
  ],
  'g_V_hasXperson_name_markoX_timesX1X_repeatXoutXcreatedXX_name': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').times(GInt(1)).repeat(Anon.out('created')).values('name'),
  ],
  'g_V_hasXperson_name_markoX_timesX0X_repeatXoutXcreatedXX_name': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').times(GInt(0)).repeat(Anon.out('created')).values('name'),
  ],
  'g_V_repeatXboth_hasXnot_productiveXX_timesX3X_constantX1X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both().has('not', 'productive')).times(GInt(3)).constant(GInt(1)),
  ],
  'g_V_hasXnot_productiveX_repeatXbothX_timesX3X_constantX1X': <Function>[
    (GraphTraversalSource g) => g.V().has('not', 'productive').repeat(Anon.both()).times(GInt(3)).constant(GInt(1)),
  ],
  'g_VX1_2_3X_repeatXboth_barrierX_emit_timesX2X_path': <Function>[
    (GraphTraversalSource g, {dynamic vid3, dynamic vid2, dynamic vid1}) => g.V(vid1, vid2, vid3).repeat(Anon.both().barrier()).emit().times(GInt(2)).path(),
  ],
  'g_V_order_byXname_descX_repeatXboth_simplePath_order_byXname_descXX_timesX2X_path': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name', order.desc).repeat(Anon.both().simplePath().order().by('name', order.desc)).times(GInt(2)).path(),
  ],
  'g_V_repeatXboth_repeatXorder_byXnameXX_timesX1XX_timesX1X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both().repeat(Anon.order().by('name')).times(GInt(1))).times(GInt(1)),
  ],
  'g_V_order_byXname_descX_repeatXlocalXout_order_byXnameXXX_timesX1X': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name', order.desc).repeat(Anon.local(Anon.out().order().by('name'))).times(GInt(1)),
  ],
  'g_V_order_byXnameX_repeatXlocalXboth_simplePath_order_byXnameXXX_timesX2X_path': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name').repeat(Anon.local(Anon.both().simplePath().order().by('name'))).times(GInt(2)).path(),
  ],
  'g_V_repeatXunionXoutXknowsX_order_byXnameX_inXcreatedX_order_byXnameXXX_timesX1X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.union(Anon.out('knows').order().by('name'), Anon.in_('created').order().by('name'))).times(GInt(1)),
  ],
  'g_V_repeatXaddV_propertyXgenerated_trueXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.addV().property('notGenerated', 'true').addV().property('notGenerated', 'true'),
    (GraphTraversalSource g) => g.V().repeat(Anon.addV().property('generated', 'true')).times(GInt(2)),
    (GraphTraversalSource g) => g.V().has('notGenerated'),
    (GraphTraversalSource g) => g.V().has('generated'),
  ],
  'g_V_repeatXdedup_bothX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.dedup().both()).times(GInt(2)),
  ],
  'g_V_repeatXaggregateXxXX_timesX2X_selectXxX_limitX1X_unfold': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.aggregate('x')).times(GInt(2)).select('x').limit(GInt(1)).unfold(),
  ],
  'g_V_valuesXstrX_repeatXsplitXabcX_conjoinX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.addV().property('str', 'ababcczababcc').addV().property('str', 'abcyabc'),
    (GraphTraversalSource g) => g.V().values('str').repeat(Anon.split('abc').conjoin('')).times(GInt(2)),
  ],
  'g_withSackX0X_V_repeatXsackXsumX_byXageX_whereXsack_isXltX59XXXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withSack(GLong(0)).V().repeat(Anon.sack(operator_.sum).by('age').where(Anon.sack().is_(P.lt(GInt(59))))).times(GInt(2)),
  ],
  'g_V_repeatXinjectXyXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.inject('y')).times(GInt(2)),
  ],
  'g_V_repeatXunionXconstantXyX_limitX1X_identityXX_timesX3X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.union(Anon.constant('y').limit(GInt(1)), Anon.identity())).times(GInt(2)),
  ],
  'g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.out().order().by('performances').tail(GInt(2))).times(GInt(1)).values('name'),
  ],
  'g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.out().order().by('performances').tail(GInt(2))).times(GInt(2)).values('name'),
  ],
  'g_VX2X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).repeat(Anon.out().local(Anon.order().by('performances').tail(GInt(1)))).times(GInt(1)).values('name'),
  ],
  'g_VX250X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid250}) => g.V(vid250).repeat(Anon.out().local(Anon.order().by('performances').tail(GInt(1)))).times(GInt(2)).values('name'),
  ],
  'g_VX3X_repeatXout_order_byXperformancesX_tailX3X_limitX1XX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.out().order().by('performances').tail(GInt(3)).limit(GInt(1))).times(GInt(2)).values('name'),
  ],
  'g_VX3X_repeatXout_order_byXperformances_descX_limitX5X_tailX1XX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.out().order().by('performances', order.desc).limit(GInt(5)).tail(GInt(1))).times(GInt(2)).values('name'),
  ],
  'g_VX3X_repeatXoutE_order_byXweightX_tailX2X_inVX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.outE().order().by('weight').tail(GInt(2)).inV()).times(GInt(2)).values('name'),
  ],
  'g_VX3X_repeatXoutE_order_byXweight_descX_limitX2X_inVX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.outE().order().by('weight', order.desc).limit(GInt(2)).inV()).times(GInt(2)).values('name'),
  ],
  'g_V_emit_repeatXout_order_byXnameXX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat(Anon.out().order().by('name')).times(GInt(2)).values('name'),
  ],
  'g_V_localXemit_repeatXout_order_byXnameXX_timesX2X_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.emit().repeat(Anon.out().order().by('name')).times(GInt(2)).values('name')),
  ],
  'g_V_emit_repeatXlocalXout_order_byXnameXXX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat(Anon.local(Anon.out().order().by('name'))).times(GInt(2)).values('name'),
  ],
  'g_V_localXemit_repeatXlocalXout_order_byXnameXXX_timesX2X_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.emit().repeat(Anon.local(Anon.out().order().by('name'))).times(GInt(2)).values('name')),
  ],
  'g_V_emitXhasLabelXpersonXX_repeatXout_order_byXnameXX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().emit(Anon.hasLabel('person')).repeat(Anon.out().order().by('name')).times(GInt(2)).values('name'),
  ],
  'g_V_untilXloops_isX2XX_repeatXout_order_byXnameXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().until(Anon.loops().is_(GInt(2))).repeat(Anon.out().order().by('name')).values('name'),
  ],
  'g_V_emit_repeatXdedupX_timesX1X': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat(Anon.dedup()).times(GInt(1)),
  ],
  'g_V_emit_repeatXdedupX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().emit().repeat(Anon.dedup()).times(GInt(2)),
  ],
  'g_unionXX': <Function>[
    (GraphTraversalSource g) => g.union(),
  ],
  'g_unionXV_name': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V().values('name')),
  ],
  'g_unionXVXv1X_VX4XX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid4, dynamic vid1}) => g.union(Anon.V(vid1), Anon.V(vid4)).values('name'),
  ],
  'g_unionXV_hasLabelXsoftwareX_V_hasLabelXpersonXX_name': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V().hasLabel('software'), Anon.V().hasLabel('person')).values('name'),
  ],
  'g_unionXV_out_out_V_hasLabelXsoftwareXX_path': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V().out().out(), Anon.V().hasLabel('software')).path(),
  ],
  'g_unionXV_out_out_V_hasLabelXsoftwareXX_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.union(Anon.V().out().out(), Anon.V().hasLabel('software')).path().by('name'),
  ],
  'g_unionXunionXV_out_outX_V_hasLabelXsoftwareXX_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.union(Anon.union(Anon.V().out().out()), Anon.V().hasLabel('software')).path().by('name'),
  ],
  'g_unionXinjectX1X_injectX2X': <Function>[
    (GraphTraversalSource g) => g.union(Anon.inject(GInt(1)), Anon.inject(GInt(2))),
  ],
  'g_V_unionXconstantX1X_constantX2X_constantX3XX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).union(Anon.constant(GInt(1)), Anon.constant(GInt(2)), Anon.constant(GInt(3))),
  ],
  'g_V_unionXout__inX_name': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.out(), Anon.in_()).values('name'),
  ],
  'g_VX1X_unionXrepeatXoutX_timesX2X__outX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).union(Anon.repeat(Anon.out()).times(GInt(2)), Anon.out()).values('name'),
  ],
  'g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.label().is_('person'), Anon.union(Anon.out().values('lang'), Anon.out().values('name')), Anon.in_().label()),
  ],
  'g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.label().is_('person'), Anon.union(Anon.out().values('lang'), Anon.out().values('name')), Anon.in_().label()).groupCount(),
  ],
  'g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.repeat(Anon.union(Anon.out('created'), Anon.in_('created'))).times(GInt(2)), Anon.repeat(Anon.union(Anon.in_('created'), Anon.out('created'))).times(GInt(2))).label().groupCount(),
  ],
  'g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1, vid2).union(Anon.outE().count(), Anon.inE().count(), Anon.outE().values('weight').sum()),
  ],
  'g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1, vid2).local(Anon.union(Anon.outE().count(), Anon.inE().count(), Anon.outE().values('weight').sum())),
  ],
  'g_VX1_2X_localXunionXcountXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1, vid2).local(Anon.union(Anon.count())),
  ],
  'g_unionXaddVXpersonX_propertyXname_aliceX_addVXpersonX_propertyXname_bobX_addVXpersonX_propertyXname_chrisX_name': <Function>[
    (GraphTraversalSource g) => g.union(Anon.addV('person').property('name', 'alice'), Anon.addV('person').property('name', 'bob'), Anon.addV('person').property('name', 'chris')).values('name'),
  ],
  'g_VX_hasLabelXpersonX_unionX_whereX_out_count_isXgtX2XXX_valuesXageX_notX_whereX_bothE_count_isXgt2XXX_valusXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').union(Anon.where(Anon.outE().count().is_(P.gt(GInt(2)))).values('age'), Anon.not_(Anon.where(Anon.outE().count().is_(P.gt(GInt(2))))).values('name')),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(123)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_mathXaddX0_5XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(10)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).math_('_ + 0.5'),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_isXgtX0XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(5)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).is_(P.gt(GInt(0))),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(2)).addV('data').property('int', GInt(3)).addV('data').property('int', GInt(4)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).sum(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_minX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(1)).addV('data').property('int', GInt(5)).addV('data').property('int', GInt(10)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).min(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_maxX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(7)).addV('data').property('int', GInt(14)).addV('data').property('int', GInt(21)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).max(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_project_byXidentityX_byXmathXmulX10XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(6)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).project('original', 'multiplied').by(Anon.identity()).by(Anon.math_('_ * 10')),
  ],
  'g_injectX99X_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(99)).asNumber(gtype.BIGDECIMAL).is_(P.typeOf(gtype.BIGDECIMAL)).groupCount(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_BIGDECIMALXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.BIGDECIMAL)),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(456)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_mathXmulX1000XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(100)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).math_('_ * 1000'),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_isXeqX42XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(42)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).is_(P.eq(GInt(42))),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(10)).addV('data').property('int', GInt(20)).addV('data').property('int', GInt(30)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).sum(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_minX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(5)).addV('data').property('int', GInt(15)).addV('data').property('int', GInt(25)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).min(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_maxX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(100)).addV('data').property('int', GInt(200)).addV('data').property('int', GInt(300)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).max(),
  ],
  'g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_project_byXidentityX_byXmathXaddX999XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(50)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).project('original', 'added').by(Anon.identity()).by(Anon.math_('_ + 999')),
  ],
  'g_injectX777X_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(777)).asNumber(gtype.BIGINT).is_(P.typeOf(gtype.BIGINT)).groupCount(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_BIGINTXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.BIGINT)),
  ],
  'g_injectXBinaryXAQIDXX': <Function>[
    (GraphTraversalSource g) => g.inject(base64Decode('AQID')),
  ],
  'g_injectXBinaryXemptyXX': <Function>[
    (GraphTraversalSource g) => g.inject(base64Decode('')),
  ],
  'g_injectXBinaryXAA_eqeqXX': <Function>[
    (GraphTraversalSource g) => g.inject(base64Decode('AA==')),
  ],
  'g_valuesXblobX_isXtypeOfXGType_BINARYXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('blob', base64Decode('AQID')),
    (GraphTraversalSource g) => g.V().values('blob').is_(P.typeOf(gtype.BINARY)),
  ],
  'g_injectXBinaryXAQIDXX_isXeqXBinaryXAQIDXXX': <Function>[
    (GraphTraversalSource g) => g.inject(base64Decode('AQID')).is_(P.eq(base64Decode('AQID'))),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(5)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_mathXaddX20XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(10)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).math_('_ + 20'),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_isXltX10XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(7)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).is_(P.lt(GInt(10))),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(1)).addV('data').property('int', GInt(2)).addV('data').property('int', GInt(3)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).sum(),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_project_byXidentityX_byXmathXmulX2XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(8)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).project('original', 'doubled').by(Anon.identity()).by(Anon.math_('_ * 2')),
  ],
  'g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_chooseXisXeqX12XX_constantXtwelveX_constantXotherXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(12)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).choose(Anon.is_(P.eq(GInt(12))), Anon.constant('twelve'), Anon.constant('other')),
  ],
  'g_injectX15X_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(15)).asNumber(gtype.BYTE).is_(P.typeOf(gtype.BYTE)).groupCount(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_BYTEXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.BYTE)),
  ],
  'g_injectXaX': <Function>[
    (GraphTraversalSource g) => g.inject(GChar('a'.runes.single)),
  ],
  'g_injectXescaped_quoteX': <Function>[
    (GraphTraversalSource g) => g.inject(GChar('\"'.runes.single)),
  ],
  'g_injectXunicodeX': <Function>[
    (GraphTraversalSource g) => g.inject(GChar('\u00E9'.runes.single)),
  ],
  'g_valuesXinitialX_isXtypeOfXGType_CHARXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('initial', GChar('a'.runes.single)),
    (GraphTraversalSource g) => g.V().values('initial').is_(P.typeOf(gtype.CHAR)),
  ],
  'g_injectXaX_isXeqXaXX': <Function>[
    (GraphTraversalSource g) => g.inject(GChar('a'.runes.single)).is_(P.eq(GChar('a'.runes.single))),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T00:00Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_project_byXidentityX_byXdateAddXDT_dayX1XX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T00:00Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)).project('original', 'nextDay').by(Anon.identity()).by(Anon.dateAdd(dt.day, GInt(1))),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_dateDiffXdatetimeX2023_08_10XX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T00:00Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)).dateDiff(DateTime.parse('2023-08-08T00:00:30Z')),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_whereXisXgtXdatetimeX2020_01_01XXXX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T12:34:56Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)).where(Anon.is_(P.gt(DateTime.parse('2020-01-01T00:00Z')))),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_chooseXisXeqXdatetimeX2023_08_08XXXX_constantXmatchX_constantXnoMatchXX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T00:00Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)).choose(Anon.is_(P.eq(DateTime.parse('2023-08-08T00:00Z'))), Anon.constant('match'), Anon.constant('noMatch')),
  ],
  'g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_localXaggregateXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.addV('event').property('datetime', DateTime.parse('2023-08-08T00:00Z')),
    (GraphTraversalSource g) => g.V().values('datetime').is_(P.typeOf(gtype.DATETIME)).local(Anon.aggregate('a')).cap('a'),
  ],
  'g_injectXdatetimeX_isXtypeOfXGType_DATETIMEXX_aggregateXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-08T00:00Z')).is_(P.typeOf(gtype.DATETIME)).aggregate('a').cap('a'),
  ],
  'g_injectXdatetimeX_isXtypeOfXGType_DATETIMEXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-08T12:34:56Z')).is_(P.typeOf(gtype.DATETIME)).groupCount(),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(1.5)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)),
  ],
  'g_E_valuesXweightX_isXtypeOfXGType_DOUBLEXX': <Function>[
    (GraphTraversalSource g) => g.E().values('weight').is_(P.typeOf(gtype.DOUBLE)),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_mathXceilX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(2.7)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).math_('ceil _'),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_isXgtX1_0XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(0.8)).addV('data').property('double', GDouble(1.2)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).is_(P.gt(GDouble(1.0))),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(1.5)).addV('data').property('double', GDouble(2.5)).addV('data').property('double', GDouble(3.5)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).sum(),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_minX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(0.1)).addV('data').property('double', GDouble(0.5)).addV('data').property('double', GDouble(0.9)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).min(),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_maxX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(2.1)).addV('data').property('double', GDouble(3.7)).addV('data').property('double', GDouble(1.9)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).max(),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_meanX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(2.1)).addV('data').property('double', GDouble(4.1)).addV('data').property('double', GDouble(6.1)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).mean(),
  ],
  'g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_order_byXascX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('double', GDouble(3.2)).addV('data').property('double', GDouble(1.8)).addV('data').property('double', GDouble(2.5)),
    (GraphTraversalSource g) => g.V().values('double').is_(P.typeOf(gtype.DOUBLE)).order().by(order.asc),
  ],
  'g_injectX5_5dX_isXtypeOfXGType_DOUBLEXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(5.5)).is_(P.typeOf(gtype.DOUBLE)).groupCount(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_DOUBLEXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.DOUBLE)),
  ],
  'g_injectXDurationX9000_0XX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 9000, microseconds: 0 ~/ 1000)),
  ],
  'g_injectXDurationX0_0XX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 0, microseconds: 0 ~/ 1000)),
  ],
  'g_injectXDurationX0_500000000XX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 0, microseconds: 500000000 ~/ 1000)),
  ],
  'g_injectXDurationX30_0XX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 30, microseconds: 0 ~/ 1000)),
  ],
  'g_injectXDurationX30_0_falseXX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 30, microseconds: 0 ~/ 1000)),
  ],
  'g_injectXDurationX1_500000000_falseXX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 1, microseconds: 500000000 ~/ 1000)),
  ],
  'g_valuesXlengthX_isXtypeOfXGType_DURATIONXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('length', Duration(seconds: 9000, microseconds: 0 ~/ 1000)),
    (GraphTraversalSource g) => g.V().values('length').is_(P.typeOf(gtype.DURATION)),
  ],
  'g_injectXDurationX9000_0XX_isXgtXDurationX3600_0XXX': <Function>[
    (GraphTraversalSource g) => g.inject(Duration(seconds: 9000, microseconds: 0 ~/ 1000)).is_(P.gt(Duration(seconds: 3600, microseconds: 0 ~/ 1000))),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(2.5)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_mathXmulX2XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(3.0)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).math_('_ * 2'),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_isXeqX1_5XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(1.5)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).is_(P.eq(GDouble(1.5))),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(1.5)).addV('data').property('float', GDouble(2.5)).addV('data').property('float', GDouble(3.0)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).sum(),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_project_byXidentityX_byXmathXmulX10XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(4.5)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).project('original', 'multiplied').by(Anon.identity()).by(Anon.math_('_ * 10')),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_whereXisXgtX1_0XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(0.5)).addV('data').property('float', GDouble(1.5)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).where(Anon.is_(P.gt(GDouble(1.0)))),
  ],
  'g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_chooseXisXeqX3_0XX_constantXthreeX_constantXotherXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('float', GDouble(3.0)),
    (GraphTraversalSource g) => g.V().values('float').asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).choose(Anon.is_(P.eq(GDouble(3.0))), Anon.constant('three'), Anon.constant('other')),
  ],
  'g_injectX2_0fX_isXtypeOfXGType_FLOATXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(2.0)).asNumber(gtype.FLOAT).is_(P.typeOf(gtype.FLOAT)).groupCount(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_FLOATXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.FLOAT)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)),
  ],
  'g_V_hasXage_typeOfXGType_INTXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.typeOf(gtype.INT)).values('name'),
  ],
  'g_V_whereXvaluesXageX_isXtypeOfXGType_INTXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.values('age').is_(P.typeOf(gtype.INT))).values('name'),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_mathXincX': <Function>[
    (GraphTraversalSource g) => g.V().values('name', 'age').is_(P.typeOf(gtype.INT)).math_('_ + 1'),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_sumX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).sum(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_minX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).min(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_maxX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).max(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_meanX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).mean(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_order_byXdescX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).order().by(order.desc),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_INTXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.INT)).groupCount(),
  ],
  'g_V_valuesXnameX_fold_isXtypeOfXGType_LISTXX_count': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().is_(P.typeOf(gtype.LIST)).count(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_LISTXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.LIST)),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', ['a', 'b', 'c']),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)),
  ],
  'g_V_hasXlist_typeOfXGType_LISTXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('name', 'test').property('list', [GInt(1), GInt(2), GInt(3)]),
    (GraphTraversalSource g) => g.V().has('list', P.typeOf(gtype.LIST)).values('name'),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX_unfold': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', ['x', 'y', 'z']),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)).unfold(),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', [GInt(1), GInt(2), GInt(3), GInt(4), GInt(5)]),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)).count(scope.local),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX_unfold_rangeX1_3X': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', ['first', 'second', 'third', 'fourth']),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)).unfold().range(GInt(1), GInt(3)),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX_project_byXidentityX_byXcountXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', ['apple', 'banana']),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)).project('original', 'size').by(Anon.identity()).by(Anon.count(scope.local)),
  ],
  'g_V_valuesXlistX_isXtypeOfXGType_LISTXX_whereXcountXlocalX_isXgtX2XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('list', [GInt(1)]).addV('data').property('list', [GInt(1), GInt(2), GInt(3)]),
    (GraphTraversalSource g) => g.V().values('list').is_(P.typeOf(gtype.LIST)).where(Anon.count(scope.local).is_(P.gt(GInt(2)))),
  ],
  'g_injectXlistX_isXtypeOfXGType_LISTXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(['test']).is_(P.typeOf(gtype.LIST)).groupCount(),
  ],
  'g_V_valuesXlongX_isXtypeOfXGType_LONGXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('long', GLong(1)),
    (GraphTraversalSource g) => g.V().values('long').is_(P.typeOf(gtype.LONG)),
  ],
  'g_V_hasXlong_typeOfXGType_LONGXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('name', 'test').property('long', GLong(1)),
    (GraphTraversalSource g) => g.V().has('long', P.typeOf(gtype.LONG)).values('name'),
  ],
  'g_V_valuesXlongX_isXtypeOfXGType_LONGXX_mathXmulX2XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('long', GLong(5)),
    (GraphTraversalSource g) => g.V().values('long').is_(P.typeOf(gtype.LONG)).math_('_ * 2'),
  ],
  'g_V_valuesXlongX_isXtypeOfXGType_LONGXX_isXgtX5XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('long', GLong(10)),
    (GraphTraversalSource g) => g.V().values('long').is_(P.typeOf(gtype.LONG)).is_(P.gt(GLong(5))),
  ],
  'g_V_valuesXlongX_isXtypeOfXGType_LONGXX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('long', GLong(1)).addV('data').property('long', GLong(2)).addV('data').property('long', GLong(3)),
    (GraphTraversalSource g) => g.V().values('long').is_(P.typeOf(gtype.LONG)).sum(),
  ],
  'g_V_valuesXlongX_isXtypeOfXGType_LONGXX_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('long', GLong(100)),
    (GraphTraversalSource g) => g.V().values('long').is_(P.typeOf(gtype.LONG)).local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_hasLabelXpersonX_valueMap_isXtypeOfXGType_MAPXX_count': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').valueMap().is_(P.typeOf(gtype.MAP)).count(),
  ],
  'g_V_groupCount_byXlabelX_isXtypeOfXGType_MAPX': <Function>[
    (GraphTraversalSource g) => g.V().groupCount().by(t.label).is_(P.typeOf(gtype.MAP)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_MAPXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.MAP)),
  ],
  'g_V_valuesXmapX_isXtypeOfXGType_MAPXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('map', {'key1': '1', 'key2': '2'}),
    (GraphTraversalSource g) => g.V().values('map').is_(P.typeOf(gtype.MAP)),
  ],
  'g_V_hasXmap_typeOfXGType_MAPXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('name', 'test').property('map', {'a': GInt(1), 'b': GInt(2)}),
    (GraphTraversalSource g) => g.V().has('map', P.typeOf(gtype.MAP)).values('name'),
  ],
  'g_V_valuesXmapX_isXtypeOfXGType_MAPXX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('map', {'a': GInt(1), 'b': GInt(2), 'c': GInt(3)}),
    (GraphTraversalSource g) => g.V().values('map').is_(P.typeOf(gtype.MAP)).count(scope.local),
  ],
  'g_V_valuesXmapX_isXtypeOfXGType_MAPXX_selectXvaluesX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('map', {'city': 'NYC', 'country': 'USA'}),
    (GraphTraversalSource g) => g.V().values('map').is_(P.typeOf(gtype.MAP)).select(column.values),
  ],
  'g_V_valuesXmapX_isXtypeOfXGType_MAPXX_whereX_countXlocalX_isXgtX1XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('map', {'single': 'value'}).addV('data').property('map', {'key1': '1', 'key2': '2'}),
    (GraphTraversalSource g) => g.V().values('map').is_(P.typeOf(gtype.MAP)).where(Anon.count(scope.local).is_(P.gt(GInt(1)))),
  ],
  'g_V_valuesXmapX_isXtypeOfXGType_MAPXX_foldX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('map', {'a': GInt(1)}).addV('data').property('map', {'b': GInt(2), 'c': GInt(3)}),
    (GraphTraversalSource g) => g.V().values('map').is_(P.typeOf(gtype.MAP)).fold(),
  ],
  'g_V_valueXnameX_aggregateXxX_capXxX_isXtypeOfXGType_SETX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').aggregate('x').cap('x').is_(P.typeOf(gtype.SET)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_SETXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.SET)),
  ],
  'g_V_valueMap_selectXkeysX_dedup_isXtypeOfXGType_SETXX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(column.keys).dedup().is_(P.typeOf(gtype.SET)),
  ],
  'g_V_valuesXsetX_isXtypeOfXGType_SETXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('set', <dynamic>{'a', 'b', 'c'}),
    (GraphTraversalSource g) => g.V().values('set').is_(P.typeOf(gtype.SET)),
  ],
  'g_V_hasXset_typeOfXGType_SETXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('name', 'test').property('set', <dynamic>{GInt(1), GInt(2), GInt(3)}),
    (GraphTraversalSource g) => g.V().has('set', P.typeOf(gtype.SET)).values('name'),
  ],
  'g_V_valuesXsetX_isXtypeOfXGType_SETXX_unfold': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('set', <dynamic>{'x', 'y', 'z'}),
    (GraphTraversalSource g) => g.V().values('set').is_(P.typeOf(gtype.SET)).unfold(),
  ],
  'g_V_valuesXsetX_isXtypeOfXGType_SETXX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('set', <dynamic>{GInt(1), GInt(2), GInt(3), GInt(4), GInt(5)}),
    (GraphTraversalSource g) => g.V().values('set').is_(P.typeOf(gtype.SET)).count(scope.local),
  ],
  'g_V_valuesXsetX_isXtypeOfXGType_SETXX_whereXcountXlocalX_isXeqX3XXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('set', <dynamic>{GInt(1), GInt(2)}).addV('data').property('set', <dynamic>{GInt(1), GInt(2), GInt(3)}),
    (GraphTraversalSource g) => g.V().values('set').is_(P.typeOf(gtype.SET)).where(Anon.count(scope.local).is_(P.eq(GInt(3)))),
  ],
  'g_V_valuesXsetX_isXtypeOfXGType_SETXX_unfold_limitX2X': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('set', <dynamic>{'first', 'second', 'third', 'fourth'}),
    (GraphTraversalSource g) => g.V().values('set').is_(P.typeOf(gtype.SET)).unfold().limit(GInt(2)),
  ],
  'g_injectXsetX_isXtypeOfXGType_SETXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(<dynamic>{'test'}).is_(P.typeOf(gtype.SET)).groupCount(),
  ],
  'g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(100)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)),
  ],
  'g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_mathXmulX10XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(50)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)).math_('_ * 10'),
  ],
  'g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_isXbetweenX20_30XX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(25)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)).is_(P.between(GInt(20), GInt(30))),
  ],
  'g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_minX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(10)).addV('data').property('int', GInt(20)).addV('data').property('int', GInt(30)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)).min(),
  ],
  'g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_maxX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('int', GInt(15)).addV('data').property('int', GInt(25)).addV('data').property('int', GInt(35)),
    (GraphTraversalSource g) => g.V().values('int').asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)).max(),
  ],
  'g_injectX42X_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_storeXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(42)).asNumber(gtype.SHORT).is_(P.typeOf(gtype.SHORT)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_SHORTXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.SHORT)),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)),
  ],
  'g_V_hasXuuid_typeOfXGType_UUIDXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('name', 'test').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().has('uuid', P.typeOf(gtype.UUID)).values('name'),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_project_byXidentityX_byXconstantXuuidXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)).project('original', 'type').by(Anon.identity()).by(Anon.constant('uuid')),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_whereXisXeqXuuidXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)).where(Anon.is_(P.eq(UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')))),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_chooseXisXeqXuuidXX_constantXmatchX_constantXnoMatchXX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)).choose(Anon.is_(P.eq(UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479'))), Anon.constant('match'), Anon.constant('noMatch')),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)).local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_aggregateXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.addV('data').property('uuid', UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
    (GraphTraversalSource g) => g.V().values('uuid').is_(P.typeOf(gtype.UUID)).aggregate('a').cap('a'),
  ],
  'g_injectXuuidX_isXtypeOfXGType_UUIDXX_groupCount': <Function>[
    (GraphTraversalSource g) => g.inject(UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')).is_(P.typeOf(gtype.UUID)).groupCount(),
  ],
  'g_injectXUUIDX47af10b_58cc_4372_a567_0f02b2f3d479XX': <Function>[
    (GraphTraversalSource g) => g.inject(UuidValue('f47af10b-58cc-4372-a567-0f02b2f3d479')),
  ],
  'g_V_aggregateXxX_byXnameX_byXageX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('x').by('name').by('age').cap('x'),
  ],
  'g_V_localXaggregateXxX_byXnameXX_byXageX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('x').by('name').by('age')).cap('x'),
  ],
  'g_V_valuesXageX_allXgtX32XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').all(P.gt(GInt(32))),
  ],
  'g_V_valuesXageX_whereXisXP_gtX33XXX_fold_allXgtX33XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').where(Anon.is_(P.gt(GInt(33)))).fold().all(P.gt(GInt(33))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_allXgtX10XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().all(P.gt(GInt(10))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_allXgtX30XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().all(P.gt(GInt(30))),
  ],
  'g_injectXabc_bcdX_allXeqXbcdXX': <Function>[
    (GraphTraversalSource g) => g.inject(['abc', 'bcd']).all(P.eq('bcd')),
  ],
  'g_injectXbcd_bcdX_allXeqXbcdXX': <Function>[
    (GraphTraversalSource g) => g.inject(['bcd', 'bcd']).all(P.eq('bcd')),
  ],
  'g_injectXnull_abcX_allXTextP_startingWithXaXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, 'abc']).all(TextP.startingWith('a')),
  ],
  'g_injectX5_8_10_10_7X_allXgteX7XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(5), GInt(8), GInt(10)], [GInt(10), GInt(7)]).all(P.gte(GInt(7))),
  ],
  'g_injectXnullX_allXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject(null).all(P.eq(null)),
  ],
  'g_injectX7X_allXeqX7XX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(7)).all(P.eq(GInt(7))),
  ],
  'g_injectXnull_nullX_allXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, null]).all(P.eq(null)),
  ],
  'g_injectX3_threeX_allXeqX3XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).all(P.eq(GInt(3))),
  ],
  'g_V_andXhasXage_gt_27X__outE_count_gte_2X_name': <Function>[
    (GraphTraversalSource g) => g.V().and_(Anon.has('age', P.gt(GInt(27))), Anon.outE().count().is_(P.gte(GInt(2)))).values('name'),
  ],
  'g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name': <Function>[
    (GraphTraversalSource g) => g.V().and_(Anon.outE(), Anon.has(t.label, 'person').and_().has('age', P.gte(GInt(32)))).values('name'),
  ],
  'g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('knows').and_().out('created').in_('created').as_('a').values('name'),
  ],
  'g_V_asXaX_andXselectXaX_selectXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').and_(Anon.select('a'), Anon.select('a')),
  ],
  'g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').and_().has('name', 'marko').and_().has('name', 'marko'),
  ],
  'g_V_valuesXageX_anyXgtX32XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').any(P.gt(GInt(32))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_anyXeqX29XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().any(P.eq(GInt(29))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_anyXgtX10XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().any(P.gt(GInt(10))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_anyXgtX42XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().any(P.gt(GInt(42))),
  ],
  'g_injectXabc_cdeX_anyXeqXbcdXX': <Function>[
    (GraphTraversalSource g) => g.inject(['abc', 'cde']).any(P.eq('bcd')),
  ],
  'g_injectXabc_bcdX_anyXeqXbcdXX': <Function>[
    (GraphTraversalSource g) => g.inject(['abc', 'bcd']).any(P.eq('bcd')),
  ],
  'g_injectXnull_abcX_anyXTextP_startingWithXaXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, 'abc']).any(TextP.startingWith('a')),
  ],
  'g_injectX5_8_10_10_7X_anyXeqX7XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(5), GInt(8), GInt(10)], [GInt(10), GInt(7)]).any(P.eq(GInt(7))),
  ],
  'g_injectXnullX_anyXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject(null).any(P.eq(null)),
  ],
  'g_injectX7X_anyXeqX7XX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(7)).any(P.eq(GInt(7))),
  ],
  'g_injectXnull_nullX_anyXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, null]).any(P.eq(null)),
  ],
  'g_injectX3_threeX_anyXeqX3XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).any(P.eq(GInt(3))),
  ],
  'g_V_coinX1_0X': <Function>[
    (GraphTraversalSource g) => g.V().coin(GDouble(1.0)),
  ],
  'g_V_coinX1X': <Function>[
    (GraphTraversalSource g) => g.V().coin(GInt(1)),
  ],
  'g_V_coinX0X': <Function>[
    (GraphTraversalSource g) => g.V().coin(GDouble(0.0)),
  ],
  'g_withStrategiesXSeedStrategyX_V_order_byXnameX_coinX50X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SeedStrategy(seed: GInt(999999))).V().order().by('name').coin(GDouble(0.5)),
  ],
  'g_VX1X_outXcreatedX_inXcreatedX_cyclicPath': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').in_('created').cyclicPath(),
  ],
  'g_VX1X_both_both_cyclicPath_byXageX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).both().both().cyclicPath().by('age'),
  ],
  'g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').in_('created').cyclicPath().path(),
  ],
  'g_VX1X_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_cyclicPath_fromXaX_toXbX_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').as_('b').in_('created').as_('c').cyclicPath().from_('a').to('b').path(),
  ],
  'g_injectX0X_V_both_coalesceXhasXname_markoX_both_constantX0XX_cyclicPath_path': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(0)).V().both().coalesce(Anon.has('name', 'marko').both(), Anon.constant(GInt(0))).cyclicPath().path(),
  ],
  'g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().out().in_().values('name').fold().dedup(scope.local).unfold(),
  ],
  'g_V_out_in_valuesXnameX_fold_dedupXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().out().map_(Anon.in_().values('name').fold().dedup(scope.local)),
  ],
  'g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().out().as_('x').in_().as_('y').select('x', 'y').by('name').fold().dedup(scope.local, 'x', 'y').unfold(),
  ],
  'g_V_both_dedup_name': <Function>[
    (GraphTraversalSource g) => g.V().both().dedup().values('name'),
  ],
  'g_V_both_hasXlabel_softwareX_dedup_byXlangX_name': <Function>[
    (GraphTraversalSource g) => g.V().both().has(t.label, 'software').dedup().by('lang').values('name'),
  ],
  'g_V_both_both_name_dedup': <Function>[
    (GraphTraversalSource g) => g.V().both().both().values('name').dedup(),
  ],
  'g_V_both_both_dedup': <Function>[
    (GraphTraversalSource g) => g.V().both().both().dedup(),
  ],
  'g_V_both_both_dedup_byXlabelX': <Function>[
    (GraphTraversalSource g) => g.V().both().both().dedup().by(t.label),
  ],
  'g_V_group_byXlabelX_byXbothE_weight_dedup_foldX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(t.label).by(Anon.bothE().values('weight').dedup().order().by(order.asc).fold()),
  ],
  'g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').both().as_('b').dedup('a', 'b').by(t.label).select('a', 'b'),
  ],
  'g_V_asXaX_out_asXbX_in_asXcX_dedupXa_bX_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addV('person').property('name', 'bob').as_('b').addV('person').property('name', 'carol').as_('c').addE('knows').from_('a').to('b').addE('likes').from_('a').to('b').addE('likes').from_('a').to('c'),
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').in_().as_('c').dedup('a', 'b').path().by('name'),
  ],
  'g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup': <Function>[
    (GraphTraversalSource g) => g.V().outE().as_('e').inV().as_('v').select('e').order().by('weight', order.asc).select('v').values('name').dedup(),
  ],
  'g_V_both_both_dedup_byXoutE_countX_name': <Function>[
    (GraphTraversalSource g) => g.V().both().both().dedup().by(Anon.outE().count()).values('name'),
  ],
  'g_V_groupCount_selectXvaluesX_unfold_dedup': <Function>[
    (GraphTraversalSource g) => g.V().groupCount().select(column.values).unfold().dedup(),
  ],
  'g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').repeat(Anon.both()).times(GInt(3)).emit().values('name').as_('b').group().by(Anon.select('a')).by(Anon.select('b').dedup().order().fold()).select(column.values).unfold().dedup(),
  ],
  'g_V_repeatXdedupX_timesX2X_count': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.dedup()).times(GInt(2)).count(),
  ],
  'g_V_both_group_by_byXout_dedup_foldX_unfold_selectXvaluesX_unfold_out_order_byXnameX_limitX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().both().group().by().by(Anon.out().dedup().fold()).unfold().select(column.values).unfold().out().order().by('name').limit(GInt(1)).values('name'),
  ],
  'g_V_bothE_properties_dedup_count': <Function>[
    (GraphTraversalSource g) => g.V().bothE().properties().dedup().count(),
  ],
  'g_V_both_properties_dedup_count': <Function>[
    (GraphTraversalSource g) => g.V().both().properties().dedup().count(),
  ],
  'g_V_both_properties_properties_dedup_count': <Function>[
    (GraphTraversalSource g) => g.V().both().properties().properties().dedup().count(),
  ],
  'g_V_order_byXname_descX_barrier_dedup_age_name': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name', order.desc).barrier().dedup().by('age').values('name'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_order_byXname_descX_barrier_dedup_age_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().order().by('name', order.desc).barrier().dedup().by('age').values('name'),
  ],
  'g_V_both_dedup_age_name': <Function>[
    (GraphTraversalSource g) => g.V().both().dedup().by('age').values('name'),
  ],
  'g_VX1X_asXaX_both_asXbX_both_asXcX_dedupXa_bX_age_selectXa_b_cX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').both().as_('b').both().as_('c').dedup('a', 'b').by('age').select('a', 'b', 'c').by('name'),
  ],
  'g_VX1X_valuesXageX_dedupXlocalX_unfold': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').dedup(scope.local).unfold(),
  ],
  'g_V_properties_dedup_count': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'josh').addV('person').property('name', 'josh').addV('person').property('name', 'josh'),
    (GraphTraversalSource g) => g.V().properties('name').dedup().count(),
  ],
  'g_V_properties_dedup_byXvalueX_count': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'josh').addV('person').property('name', 'josh').addV('person').property('name', 'josh'),
    (GraphTraversalSource g) => g.V().properties('name').dedup().by(t.value_).count(),
  ],
  'g_V_both_hasXlabel_softwareX_dedup_byXlangX_byXnameX_name': <Function>[
    (GraphTraversalSource g) => g.V().both().has(t.label, 'software').dedup().by('lang').by('name').values('name'),
  ],
  'g_V_count_discard': <Function>[
    (GraphTraversalSource g) => g.V().count().discard(),
  ],
  'g_V_hasLabelXpersonX_discard': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').discard(),
  ],
  'g_VX1X_outXcreatedX_discard': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').discard(),
  ],
  'g_V_discard': <Function>[
    (GraphTraversalSource g) => g.V().discard(),
  ],
  'g_V_discard_discard': <Function>[
    (GraphTraversalSource g) => g.V().discard().discard(),
  ],
  'g_V_discard_fold': <Function>[
    (GraphTraversalSource g) => g.V().discard().fold(),
  ],
  'g_V_discard_fold_discard': <Function>[
    (GraphTraversalSource g) => g.V().discard().fold().discard(),
  ],
  'g_V_discard_fold_constantX1X': <Function>[
    (GraphTraversalSource g) => g.V().discard().fold().constant(GInt(1)),
  ],
  'g_V_projectXxX_byXcoalesceXage_isXgtX29XX_discardXX_selectXxX': <Function>[
    (GraphTraversalSource g) => g.V().project('x').by(Anon.coalesce(Anon.values('age').is_(P.gt(GInt(29))), Anon.discard())).select('x'),
  ],
  'g_V_drop': <Function>[
    (GraphTraversalSource g) => g.addV().as_('a').addV().as_('b').addE('knows').to('a'),
    (GraphTraversalSource g) => g.V().drop(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_V_outE_drop': <Function>[
    (GraphTraversalSource g) => g.addV().as_('a').addV().as_('b').addE('knows').to('a'),
    (GraphTraversalSource g) => g.V().outE().drop(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_V_properties_drop': <Function>[
    (GraphTraversalSource g) => g.addV().property('name', 'bob').addV().property('name', 'alice'),
    (GraphTraversalSource g) => g.V().properties().drop(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.V().properties(),
  ],
  'g_E_propertiesXweightX_drop': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.E().properties('weight').drop(),
    (GraphTraversalSource g) => g.E().properties(),
  ],
  'g_V_properties_propertiesXstartTimeX_drop': <Function>[
    (GraphTraversalSource g) => g.addV().property('name', 'bob').property(cardinality.list, 'location', 'ny', 'startTime', GInt(2014), 'endTime', GInt(2016)).property(cardinality.list, 'location', 'va', 'startTime', GInt(2016)).addV().property('name', 'alice').property(cardinality.list, 'location', 'va', 'startTime', GInt(2014), 'endTime', GInt(2016)).property(cardinality.list, 'location', 'ny', 'startTime', GInt(2016)),
    (GraphTraversalSource g) => g.V().properties().properties('startTime').drop(),
    (GraphTraversalSource g) => g.V().properties().properties(),
    (GraphTraversalSource g) => g.V().properties().properties('startTime'),
  ],
  'g_V_filterXisX0XX': <Function>[
    (GraphTraversalSource g) => g.V().filter_(Anon.is_(GInt(0))),
  ],
  'g_V_filterXconstantX0XX': <Function>[
    (GraphTraversalSource g) => g.V().filter_(Anon.constant(GInt(0))),
  ],
  'g_V_filterXhasXlang_javaXX': <Function>[
    (GraphTraversalSource g) => g.V().filter_(Anon.has('lang', 'java')),
  ],
  'g_VX1X_filterXhasXage_gtX30XXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).filter_(Anon.has('age', P.gt(GInt(30)))),
  ],
  'g_VX2X_filterXhasXage_gtX30XXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).filter_(Anon.has('age', P.gt(GInt(30)))),
  ],
  'g_VX1X_out_filterXhasXage_gtX30XXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().filter_(Anon.has('age', P.gt(GInt(30)))),
  ],
  'g_V_filterXhasXname_startingWithXm_or_pXX': <Function>[
    (GraphTraversalSource g) => g.V().filter_(Anon.has('name', TextP.startingWith('m').or_(TextP.startingWith('p')))),
  ],
  'g_E_filterXisX0XX': <Function>[
    (GraphTraversalSource g) => g.E().filter_(Anon.is_(GInt(0))),
  ],
  'g_E_filterXconstantX0XX': <Function>[
    (GraphTraversalSource g) => g.E().filter_(Anon.constant(GInt(0))),
  ],
  'g_VX1X_hasXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('name'),
  ],
  'g_VX1X_hasXcircumferenceX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('circumference'),
  ],
  'g_VX1X_hasXname_markoX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('name', 'marko'),
  ],
  'g_VX1X_hasXname_markovarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1}) => g.V(vid1).has('name', xx1),
  ],
  'g_VX2X_hasXname_markoX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('name', 'marko'),
  ],
  'g_V_hasXname_markoX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko'),
  ],
  'g_V_hasXname_blahX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'blah'),
  ],
  'g_V_hasXage_gt_30X': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.gt(GInt(30))),
  ],
  'g_VX1X_hasXage_gt_30X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('age', P.gt(GInt(30))),
  ],
  'g_V_hasXpersonvar_age_gt_30X': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has(xx1, 'age', P.gt(GInt(30))),
  ],
  'g_VX4X_hasXage_gt_30X': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).has('age', P.gt(GInt(30))),
  ],
  'g_VXv1X_hasXage_gt_30X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).has('age', P.gt(GInt(30))),
  ],
  'g_VXv4X_hasXage_gt_30X': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).has('age', P.gt(GInt(30))),
  ],
  'g_VX1X_out_hasXid_2X': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).has('age', P.gt(GInt(30))),
  ],
  'g_V_hasXblahX': <Function>[
    (GraphTraversalSource g) => g.V().has('blah'),
  ],
  'g_V_hasXperson_name_markoX_age': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').values('age'),
  ],
  'g_V_hasXperson_name_markovarX_age': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', xx1).values('age'),
  ],
  'g_V_hasXpersonvar_name_markoX_age': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has(xx1, 'name', 'marko').values('age'),
  ],
  'g_VX1X_outE_hasXweight_inside_0_06X_inV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().has('weight', P.inside(GDouble(0.0), GDouble(0.6))).inV(),
  ],
  'g_EX11X_outV_outE_hasXid_10X': <Function>[
    (GraphTraversalSource g, {dynamic eid11, dynamic eid10}) => g.E(eid11).outV().outE().has(t.id, eid10),
  ],
  'g_EX11X_outV_outE_hasXid_10AsStringX': <Function>[
    (GraphTraversalSource g, {dynamic eid11, dynamic eid10}) => g.E(eid11).outV().outE().has(t.id, eid10),
  ],
  'g_V_hasXlocationX': <Function>[
    (GraphTraversalSource g) => g.V().has('location'),
  ],
  'g_V_hasXage_withinX27X_count': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.within(GInt(27))).count(),
  ],
  'g_V_hasXage_withinX27_nullX_count': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.within(GInt(27), null)).count(),
  ],
  'g_V_hasXage_withinX27_29X_count': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.within(GInt(27), GInt(29))).count(),
  ],
  'g_V_hasXage_withoutX27X_count': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.without(GInt(27))).count(),
  ],
  'g_V_hasXage_withoutX27_29X_count': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.without(GInt(27), GInt(29))).count(),
  ],
  'g_V_hasXperson_age_withinX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'age', P.within()),
  ],
  'g_V_hasXperson_age_withoutX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'age', P.without()),
  ],
  'g_V_hasXname_containingXarkXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.containing('ark')),
  ],
  'g_V_hasXname_startingWithXmarXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.startingWith('mar')),
  ],
  'g_V_hasXname_endingWithXasXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.endingWith('as')),
  ],
  'g_V_hasXperson_name_containingXoX_andXltXmXXX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', TextP.containing('o').and_(P.lt('m'))),
  ],
  'g_V_hasXname_gtXmX_andXcontainingXoXXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', P.gt('m').and_(TextP.containing('o'))),
  ],
  'g_V_hasXname_not_containingXarkXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.notContaining('ark')),
  ],
  'g_V_hasXname_not_startingWithXmarXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.notStartingWith('mar')),
  ],
  'g_V_hasXname_not_endingWithXasXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.notEndingWith('as')),
  ],
  'g_V_hasXname_regexXrMarXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.regex('^mar')),
  ],
  'g_V_hasXname_notRegexXrMarXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', TextP.notRegex('^mar')),
  ],
  'g_V_hasXname_regexXTinkerXX': <Function>[
    (GraphTraversalSource g) => g.addV('software').property('name', 'Apache TinkerPop©'),
    (GraphTraversalSource g) => g.V().has('name', TextP.regex('Tinker')).values('name'),
  ],
  'g_V_hasXname_regexXTinkerUnicodeXX': <Function>[
    (GraphTraversalSource g) => g.addV('software').property('name', 'Apache TinkerPop©'),
    (GraphTraversalSource g) => g.V().has('name', TextP.regex('Tinker.*\u00A9')).values('name'),
  ],
  'g_V_hasXp_neqXvXX': <Function>[
    (GraphTraversalSource g) => g.V().has('p', P.neq('v')),
  ],
  'g_V_hasXage_gtX18X_andXltX30XXorXgtx35XXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.gt(GInt(18)).and_(P.lt(GInt(30))).or_(P.gt(GInt(35)))),
  ],
  'g_V_hasXage_gtX18X_andXltX30XXorXltx35XXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age', P.gt(GInt(18)).and_(P.lt(GInt(30))).and_(P.lt(GInt(35)))),
  ],
  'g_V_hasXk_withinXcXX_valuesXkX': <Function>[
    (GraphTraversalSource g) => g.addV().property('k', '轉注').addV().property('k', '✦').addV().property('k', '♠').addV().property('k', 'A'),
    (GraphTraversalSource g) => g.V().has('k', P.within('轉注', '✦', '♠')).values('k'),
  ],
  'g_V_hasXnullX': <Function>[
    (GraphTraversalSource g) => g.V().has(null),
  ],
  'g_V_hasXnull_testnullkeyX': <Function>[
    (GraphTraversalSource g) => g.V().has(null, 'test-null-key'),
  ],
  'g_E_hasXnullX': <Function>[
    (GraphTraversalSource g) => g.E().has(null),
  ],
  'g_V_hasXlabel_personX': <Function>[
    (GraphTraversalSource g) => g.V().has(t.label, 'person'),
  ],
  'g_V_hasXlabel_eqXpersonXX': <Function>[
    (GraphTraversalSource g) => g.V().has(t.label, P.eq('person')),
  ],
  'g_V_hasXname_nullX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', null),
  ],
  'g_V_hasIdXemptyX_count': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasId(xx1).count(),
  ],
  'g_V_hasIdXwithinXemptyXX_count': <Function>[
    (GraphTraversalSource g) => g.V().hasId(P.within([])).count(),
  ],
  'g_V_hasIdXwithoutXemptyXX_count': <Function>[
    (GraphTraversalSource g) => g.V().hasId(P.without([])).count(),
  ],
  'g_V_notXhasIdXwithinXemptyXXX_count': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.hasId(P.within([]))).count(),
  ],
  'g_V_hasIdXnullX': <Function>[
    (GraphTraversalSource g) => g.V().hasId(null),
  ],
  'g_V_hasIdXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().hasId(P.eq(null)),
  ],
  'g_V_hasIdX2_nullX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V().hasId(vid2, null),
  ],
  'g_V_hasIdXmarkovar_vadasvarX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().hasId(vid1, vid2),
  ],
  'g_V_hasIdXmarkovar_vadasvar_petervarX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().hasId(vid1, vid2),
  ],
  'g_V_hasIdX2AsString_nullX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V().hasId(vid2, null),
  ],
  'g_V_hasIdX1AsString_2AsString_nullX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().hasId(vid1, vid2, null),
  ],
  'g_V_hasIdXnull_2X': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V().hasId(null, vid2),
  ],
  'g_V_hasIdX1X_hasIdX2X': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().hasId(vid1).hasId(vid2),
  ],
  'g_V_in_hasIdXneqX1XX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().in_().hasId(P.neq(xx1)),
  ],
  'g_VX1X_out_hasIdX2X': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1).out().hasId(vid2),
  ],
  'g_VX1X_out_hasXid_2_3X': <Function>[
    (GraphTraversalSource g, {dynamic vid3, dynamic vid2, dynamic vid1}) => g.V(vid1).out().hasId(vid2, vid3),
  ],
  'g_VX1X_out_hasXid_2AsString_3AsStringX': <Function>[
    (GraphTraversalSource g, {dynamic vid3, dynamic vid2, dynamic vid1}) => g.V(vid1).out().hasId(vid2, vid3),
  ],
  'g_VX1AsStringX_out_hasXid_2AsStringX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1).out().hasId(vid2),
  ],
  'g_VX1X_out_hasXid_2_3X_inList': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1}) => g.V(vid1).out().hasId(xx1),
  ],
  'g_V_hasXid_1_2X': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().hasId(vid1, vid2),
  ],
  'g_V_hasXid_1_2X_inList': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasId(xx1),
  ],
  'g_V_both_dedup_properties_hasKeyXageX_value': <Function>[
    (GraphTraversalSource g) => g.V().both().properties().dedup().hasKey('age').value_(),
  ],
  'g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value': <Function>[
    (GraphTraversalSource g) => g.V().both().properties().dedup().hasKey('age').hasValue(P.gt(GInt(30))).value_(),
    (GraphTraversalSource g) => g.V().both().properties().dedup().hasKey('age').hasValue(P.gt(GInt(30))).value_(),
  ],
  'g_V_bothE_properties_dedup_hasKeyXweightX_value': <Function>[
    (GraphTraversalSource g) => g.V().bothE().properties().dedup().hasKey('weight').value_(),
  ],
  'g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value': <Function>[
    (GraphTraversalSource g) => g.V().bothE().properties().dedup().hasKey('weight').hasValue(P.lt(GDouble(0.3))).value_(),
    (GraphTraversalSource g) => g.V().bothE().properties().dedup().hasKey('weight').hasValue(P.lt(GDouble(0.3))).value_(),
  ],
  'g_V_properties_hasKeyXnullX': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasKey(null),
  ],
  'g_V_properties_hasKeyXnull_nullX': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasKey(null, null),
  ],
  'g_V_properties_hasKeyXnull_ageX_value': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasKey(null, 'age').value_(),
  ],
  'g_E_properties_hasKeyXnullX': <Function>[
    (GraphTraversalSource g) => g.E().properties().hasKey(null),
  ],
  'g_E_properties_hasKeyXnull_nullX': <Function>[
    (GraphTraversalSource g) => g.E().properties().hasKey(null, null),
  ],
  'g_E_properties_hasKeyXnull_weightX_value': <Function>[
    (GraphTraversalSource g) => g.E().properties().hasKey(null, 'weight').value_(),
  ],
  'g_EX7X_hasLabelXknowsX': <Function>[
    (GraphTraversalSource g, {dynamic eid7}) => g.E(eid7).hasLabel('knows'),
  ],
  'g_E_hasLabelXknowsX': <Function>[
    (GraphTraversalSource g) => g.E().hasLabel('knows'),
  ],
  'g_E_hasLabelXuses_traversesX': <Function>[
    (GraphTraversalSource g) => g.E().hasLabel('uses', 'traverses'),
  ],
  'g_V_hasLabelXperson_software_blahX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person', 'software', 'blah'),
  ],
  'g_V_hasLabelXperson_softwarevarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasLabel('person', xx1),
  ],
  'g_V_hasLabelXpersonX_hasLabelXsoftwareX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').hasLabel('software'),
  ],
  'g_V_hasLabelXpersonvarX_hasLabelXsoftwareX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasLabel(xx1).hasLabel('software'),
  ],
  'g_V_hasLabelXpersonvar_softwarevarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().hasLabel(xx1, xx2),
  ],
  'g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').has('age', P.not_(P.lte(GInt(10)).and_(P.not_(P.between(GInt(11), GInt(20))))).and_(P.lt(GInt(29)).or_(P.eq(GInt(35))))).values('name'),
  ],
  'g_V_hasLabelXnullX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel(null),
  ],
  'g_V_hasXlabel_nullX': <Function>[
    (GraphTraversalSource g) => g.V().has(t.label, null),
  ],
  'g_V_hasLabelXnull_nullX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel(null, null),
  ],
  'g_V_hasLabelXnull_personX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel(null, 'person'),
  ],
  'g_E_hasLabelXnullX': <Function>[
    (GraphTraversalSource g) => g.E().hasLabel(null),
  ],
  'g_E_hasXlabel_nullX': <Function>[
    (GraphTraversalSource g) => g.E().has(t.label, null),
  ],
  'g_V_properties_hasLabelXnullX': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasLabel(null),
  ],
  'g_V_hasNotXageX_name': <Function>[
    (GraphTraversalSource g) => g.V().hasNot('age').values('name'),
  ],
  'g_V_properties_hasValueXnullX': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasValue(null),
  ],
  'g_V_properties_hasValueXnull_nullX': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasValue(null, null),
  ],
  'g_V_properties_hasValueXnull_joshX_value': <Function>[
    (GraphTraversalSource g) => g.V().properties().hasValue(null, 'josh').value_(),
  ],
  'g_V_valuesXageX_isX32X': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(GInt(32)),
  ],
  'g_V_valuesXageX_isX32varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().values('age').is_(xx1),
  ],
  'g_V_valuesXageX_isXlte_30X': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.lte(GInt(30))),
  ],
  'g_V_valuesXageX_isXlte_30varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().values('age').is_(P.lte(xx1)),
  ],
  'g_V_valuesXageX_isXgte_29X_isXlt_34X': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.gte(GInt(29))).is_(P.lt(GInt(34))),
  ],
  'g_V_valuesXageX_isXgte_29vaarX_isXlt_34varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().values('age').is_(P.gte(xx1)).is_(P.lt(xx2)),
  ],
  'g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.in_('created').count().is_(GInt(1))).values('name'),
  ],
  'g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.in_('created').count().is_(P.gte(GInt(2)))).values('name'),
  ],
  'g_V_valuesXageX_noneXgtX32XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').none(P.gt(GInt(32))),
  ],
  'g_V_valuesXageX_whereXisXP_gtX33XXX_fold_noneXlteX33XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').where(Anon.is_(P.gt(GInt(33)))).fold().none(P.lte(GInt(33))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_noneXltX10XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().none(P.lt(GInt(10))),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_noneXgtX30XX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().none(P.gt(GInt(30))),
  ],
  'g_injectXabc_bcdX_noneXeqXbcdXX': <Function>[
    (GraphTraversalSource g) => g.inject(['abc', 'bcd']).none(P.eq('bcd')),
  ],
  'g_injectXbcd_bcdX_noneXeqXabcXX': <Function>[
    (GraphTraversalSource g) => g.inject(['bcd', 'bcd']).none(P.eq('abc')),
  ],
  'g_injectXnull_bcdX_noneXP_eqXabcXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, 'bcd']).none(P.eq('abc')),
  ],
  'g_injectX5_8_10_10_7X_noneXltX7XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(5), GInt(8), GInt(10)], [GInt(10), GInt(7)]).none(P.lt(GInt(7))),
  ],
  'g_injectXnullX_noneXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject(null).none(P.eq(null)),
  ],
  'g_injectX7X_noneXeqX7XX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(7)).none(P.eq(GInt(7))),
  ],
  'g_injectXnull_1_emptyX_noneXeqXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, GInt(1)], []).none(P.eq(null)),
  ],
  'g_injectXnull_nullX_noneXnotXnullXX': <Function>[
    (GraphTraversalSource g) => g.inject([null, null]).none(P.neq(null)),
  ],
  'g_injectX3_threeX_noneXeqX3XX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).none(P.eq(GInt(3))),
  ],
  'g_V_notXhasXage_gt_27XX_name': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.has('age', P.gt(GInt(27)))).values('name'),
  ],
  'g_V_notXnotXhasXage_gt_27XXX_name': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.not_(Anon.has('age', P.gt(GInt(27))))).values('name'),
  ],
  'g_V_notXhasXname_gt_27XX_name': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.has('name', P.gt(GInt(27)))).values('name'),
  ],
  'g_V_orXhasXage_gt_27X__outE_count_gte_2X_name': <Function>[
    (GraphTraversalSource g) => g.V().or_(Anon.has('age', P.gt(GInt(27))), Anon.outE().count().is_(P.gte(GInt(2)))).values('name'),
  ],
  'g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name': <Function>[
    (GraphTraversalSource g) => g.V().or_(Anon.outE('knows'), Anon.has(t.label, 'software').or_().has('age', P.gte(GInt(35)))).values('name'),
  ],
  'g_V_asXaX_orXselectXaX_selectXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').or_(Anon.select('a'), Anon.select('a')),
  ],
  'g_VX1X_out_limitX2X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().limit(GInt(2)),
  ],
  'g_VX1X_out_limitX2varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1}) => g.V(vid1).out().limit(xx1),
  ],
  'g_V_localXoutE_limitX1X_inVX_limitX3X': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.outE().limit(GInt(1))).inV().limit(GInt(3)),
  ],
  'g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('knows').outE('created').range(GInt(0), GInt(1)).inV(),
  ],
  'g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('knows').out('created').range(GInt(0), GInt(1)),
  ],
  'g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').in_('created').range(GInt(1), GInt(3)),
  ],
  'g_VX1X_outXcreatedX_inXcreatedX_rangeX1var_3varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2, dynamic vid1}) => g.V(vid1).out('created').in_('created').range(xx1, xx2),
  ],
  'g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').inE('created').range(GInt(1), GInt(3)).outV(),
  ],
  'g_V_repeatXbothX_timesX3X_rangeX5_11X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both()).times(GInt(3)).range(GInt(5), GInt(11)),
  ],
  'g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').in_().as_('b').in_().as_('c').select('a', 'b', 'c').by('name').limit(scope.local, GInt(2)),
  ],
  'g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('a').in_().as_('b').in_().as_('c').select('a', 'b', 'c').by('name').limit(scope.local, xx1),
  ],
  'g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').in_().as_('b').in_().as_('c').select('a', 'b', 'c').by('name').limit(scope.local, GInt(1)),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').range(scope.local, GInt(1), GInt(3)),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1var_3varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').range(scope.local, xx1, xx2),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').range(scope.local, GInt(1), GInt(2)),
  ],
  'g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').order().by('age').skip(GInt(1)).values('name'),
  ],
  'g_V_hasLabelXpersonX_order_byXageX_skipX1varX_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasLabel('person').order().by('age').skip(xx1).values('name'),
  ],
  'g_V_foldX_rangeXlocal_6_7X': <Function>[
    (GraphTraversalSource g) => g.V().fold().range(scope.local, GInt(6), GInt(7)),
  ],
  'g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').fold().order(scope.local).skip(scope.local, GInt(2)),
  ],
  'g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().outE().values('weight').fold().order(scope.local).skip(scope.local, xx1),
  ],
  'g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').order().by('age').values('name').skip(GInt(1)),
  ],
  'g_VX1X_valuesXageX_rangeXlocal_20_30X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').range(scope.local, GInt(20), GInt(30)),
  ],
  'g_V_mapXin_hasIdX1XX_limitX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V().map_(Anon.in_().hasId(vid1)).limit(GInt(2)).values('name'),
  ],
  'g_V_rangeX2_1X': <Function>[
    (GraphTraversalSource g) => g.V().range(GInt(2), GInt(1)),
  ],
  'g_V_rangeX3_2X': <Function>[
    (GraphTraversalSource g) => g.V().range(GInt(3), GInt(2)),
  ],
  'g_injectXlistX1_2_3XX_rangeXlocal_1_2X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)]).range(scope.local, GInt(1), GInt(2)),
  ],
  'g_injectXlistX1_2_3XX_limitXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)]).limit(scope.local, GInt(1)),
  ],
  'g_injectXlistX1_2_3X_limitXlocal_1X_unfold': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)]).limit(scope.local, GInt(1)).unfold(),
  ],
  'g_injectX1_2_3_4_5X_limitXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)], [GInt(3), GInt(4), GInt(5)]).limit(scope.local, GInt(1)),
  ],
  'g_injectX1_2_3_4_5_6X_rangeXlocal_1_2X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)], [GInt(4), GInt(5), GInt(6)]).range(scope.local, GInt(1), GInt(2)),
  ],
  'g_VX5X_repeatXlimitX1X_inX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.limit(GInt(1)).in_()).times(GInt(2)).values('name'),
  ],
  'g_VX5X_repeatXlimitX1X_inX_untilXloopsXisX2XXX_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.limit(GInt(1)).in_()).until(Anon.loops().is_(GInt(2))).values('name'),
  ],
  'g_VX5X_limitX1X_in_limitX1X_in_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).limit(GInt(1)).in_().limit(GInt(1)).in_().values('name'),
  ],
  'g_VX5X_repeatXlimitX1X_inX_timesX1X_repeatXlimitX1X_inX_timesX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.limit(GInt(1)).in_()).times(GInt(1)).repeat(Anon.limit(GInt(1)).in_()).times(GInt(1)).values('name'),
  ],
  'g_VX5X_repeatXlimitX1X_in_aggregateXxXX_timesX2X_capXxX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.limit(GInt(1)).in_().aggregate('x')).times(GInt(2)).cap('x'),
  ],
  'g_VX5X_repeatXrangeX0_1X_inX_timesX2X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.range(GInt(0), GInt(1)).in_()).times(GInt(2)).values('name'),
  ],
  'g_VX5X_repeatXrangeX0_1X_inX_untilXloopsXisX2XXX_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.range(GInt(0), GInt(1)).in_()).until(Anon.loops().is_(GInt(2))).values('name'),
  ],
  'g_VX5X_rangeX0_1X_in_rangeX0_1X_in_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).range(GInt(0), GInt(1)).in_().range(GInt(0), GInt(1)).in_().values('name'),
  ],
  'g_VX5X_repeatXrangeX0_1X_in_repeatXrangeX0_1X_inX_timesX1XX_timesX1X_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.range(GInt(0), GInt(1)).in_().repeat(Anon.range(GInt(0), GInt(1)).in_()).times(GInt(1))).times(GInt(1)).values('name'),
  ],
  'g_VX5X_repeatXrangeX0_1X_in_aggregateXxXX_timesX2X_capXxX': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.V(vid5).repeat(Anon.range(GInt(0), GInt(1)).in_().aggregate('x')).times(GInt(2)).cap('x'),
  ],
  'g_withoutStrategiesXEarlyLimitStrategyX_VX5X_repeatXlimitX1X_in_limitX1X_limitX1XX_timesX2X': <Function>[
    (GraphTraversalSource g, {dynamic vid5}) => g.withoutStrategies(EarlyLimitStrategy).V(vid5).repeat(Anon.limit(GInt(1)).in_().limit(GInt(1)).limit(GInt(1))).times(GInt(2)),
  ],
  'g_V_repeatXout_whereXhasXnameX_order_byXnameX_limitX1XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out().where(Anon.has('name').order().by('name').limit(GInt(1)))).times(GInt(2)),
  ],
  'g_V_out_whereXhasXnameX_order_byXnameX_limitX1XX_out_whereXhasXnameX_order_byXnameX_limitX1XX': <Function>[
    (GraphTraversalSource g) => g.V().out().where(Anon.has('name').order().by('name').limit(GInt(1))).out().where(Anon.has('name').order().by('name').limit(GInt(1))),
  ],
  'g_V_hasXnameXJAMXX_repeatXoutXfollowedByX_order_byXnameX_limitX2XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'JAM').repeat(Anon.out('followedBy').order().by('name').limit(GInt(2))).times(GInt(2)),
  ],
  'g_V_hasXnameXJAMXX_outXfollowedByX_order_byXnameX_limitX2X_outXfollowedByX_order_byXnameX_limitX2X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'JAM').out('followedBy').order().by('name').limit(GInt(2)).out('followedBy').order().by('name').limit(GInt(2)),
  ],
  'g_V_hasXnameXDRUMSXX_repeatXinXfollowedByX_order_byXnameX_rangeX1_4XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'DRUMS').repeat(Anon.in_('followedBy').order().by('name').range(GInt(1), GInt(4))).times(GInt(2)),
  ],
  'g_V_hasXnameXDRUMSXX_inXfollowedByX_order_byXnameX_rangeX1_4X_inXfollowedByX_order_byXnameX_rangeX1_4X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'DRUMS').in_('followedBy').order().by('name').range(GInt(1), GInt(4)).in_('followedBy').order().by('name').range(GInt(1), GInt(4)),
  ],
  'g_V_chooseXvaluesXageX_isXlteX30XX_out_order_byXnameX_limitX1X_out_order_byXnameX_limitX2XX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age').is_(P.lte(GInt(30))), Anon.out().order().by('name').limit(GInt(1)), Anon.out().order().by('name').limit(GInt(2))),
  ],
  'g_V_chooseXvaluesXageX_isXlteX30XX_localXout_order_byXnameX_limitX1XX_localXout_order_byXnameX_limitX2XXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.values('age').is_(P.lte(GInt(30))), Anon.local(Anon.out().order().by('name').limit(GInt(1))), Anon.local(Anon.out().order().by('name').limit(GInt(2)))),
  ],
  'g_V_hasXnameXHEY_BO_DIDDLEYXX_unionXoutXfollowedByX_order_byXnameX_limitX2X_outXsungByX_order_byXnameX_byXnameX_limitX1XX_unionXoutXfollowedByX_order_limitX2X_outXsungByX_order_byXnameX_limitX1XX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'HEY BO DIDDLEY').union(Anon.out('followedBy').order().by('name').limit(GInt(2)), Anon.out('sungBy').order().by('name').limit(GInt(1))).union(Anon.out('followedBy').order().by('name').limit(GInt(2)), Anon.out('sungBy').order().by('name').limit(GInt(1))),
  ],
  'g_V_hasXnameXHEY_BO_DIDDLEYXX_repeatXunionXoutXfollowedByX_order_byXnameX_limitX2X_outXsungByX_order_byXnameX_limitX1XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'HEY BO DIDDLEY').repeat(Anon.union(Anon.out('followedBy').order().by('name').limit(GInt(2)), Anon.out('sungBy').order().by('name').limit(GInt(1)))).times(GInt(2)),
  ],
  'g_V_sampleX1X_byXageX_byXT_idX': <Function>[
    (GraphTraversalSource g) => g.V().sample(GInt(1)).by('age').by(t.id),
  ],
  'g_E_sampleX1X': <Function>[
    (GraphTraversalSource g) => g.E().sample(GInt(1)),
  ],
  'g_E_sampleX2X_byXweightX': <Function>[
    (GraphTraversalSource g) => g.E().sample(GInt(2)).by('weight'),
  ],
  'g_V_localXoutE_sampleX1X_byXweightXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.outE().sample(GInt(1)).by('weight')),
  ],
  'g_withStrategiesXSeedStrategyX_V_group_byXlabelX_byXbothE_weight_order_sampleX2X_foldXunfold': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SeedStrategy(seed: GInt(999999))).V().group().by(t.label).by(Anon.bothE().values('weight').order().sample(GInt(2)).fold()).unfold(),
  ],
  'g_withStrategiesXSeedStrategyX_V_group_byXlabelX_byXbothE_weight_order_fold_sampleXlocal_5XXunfold': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SeedStrategy(seed: GInt(999999))).V().group().by(t.label).by(Anon.bothE().values('weight').order().fold().sample(scope.local, GInt(5))).unfold(),
  ],
  'g_withStrategiesXSeedStrategyX_V_order_byXlabel_descX_sampleX1X_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SeedStrategy(seed: GInt(999999))).V().order().by(t.label, order.desc).sample(GInt(1)).by('age'),
  ],
  'g_VX1X_valuesXageX_sampleXlocal_5X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').sample(scope.local, GInt(5)),
  ],
  'g_V_repeatXsampleX2XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.sample(GInt(2))).times(GInt(2)),
  ],
  'g_V_sampleX2X_sampleX2X': <Function>[
    (GraphTraversalSource g) => g.V().sample(GInt(2)).sample(GInt(2)),
  ],
  'g_V3_repeatXout_order_byXperformancesX_sampleX2X_aggregateXxXX_untilXloops_isX2XX_capXxX_unfold': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).repeat(Anon.out().order().by('performances').sample(GInt(2)).aggregate('x')).until(Anon.loops().is_(GInt(2))).cap('x').unfold(),
  ],
  'g_V3_out_order_byXperformancesX_sampleX2X_aggregateXxX_out_order_byXperformancesX_sampleX2X_aggregateXxX_capXxX_unfold': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).out().order().by('performances').sample(GInt(2)).aggregate('x').out().order().by('performances').sample(GInt(2)).aggregate('x').cap('x').unfold(),
  ],
  'g_VX1X_outXcreatedX_inXcreatedX_simplePath': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').in_('created').simplePath(),
  ],
  'g_V_repeatXboth_simplePathX_timesX3X_path': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both().simplePath()).times(GInt(3)).path(),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').simplePath().by(t.label).from_('b').to('c').path().by('name'),
  ],
  'g_injectX0X_V_both_coalesceXhasXname_markoX_both_constantX0XX_simplePath_path': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(0)).V().both().coalesce(Anon.has('name', 'marko').both(), Anon.constant(GInt(0))).simplePath().path(),
  ],
  'g_V_both_asXaX_both_asXbX_simplePath_path_byXageX__fromXaX_toXbX': <Function>[
    (GraphTraversalSource g) => g.V().both().as_('a').both().as_('b').simplePath().path().by('age').from_('a').to('b'),
  ],
  'g_V_valuesXnameX_order_tailXglobal_2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().tail(scope.global, GInt(2)),
  ],
  'g_V_valuesXnameX_order_tailX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().tail(GInt(2)),
  ],
  'g_V_valuesXnameX_order_tailX2varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().values('name').order().tail(xx1),
  ],
  'g_V_valuesXnameX_order_tail': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().tail(),
  ],
  'g_V_valuesXnameX_order_tailX7X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().tail(GInt(7)),
  ],
  'g_V_repeatXbothX_timesX3X_tailX7X': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both()).times(GInt(3)).tail(GInt(7)),
  ],
  'g_V_repeatXin_outX_timesX3X_tailX7X_count': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.in_().out()).times(GInt(3)).tail(GInt(7)).count(),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X_unfold': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select('a').by(Anon.unfold().values('name').fold()).tail(scope.local, GInt(1)).unfold(),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select('a').by(Anon.unfold().values('name').fold()).tail(scope.local).unfold(),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').tail(scope.local, GInt(2)),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').tail(scope.local, xx1),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').select('a', 'b', 'c').by('name').tail(scope.local, GInt(1)),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_1X_unfold': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.mixed, 'a').by(Anon.unfold().values('name').fold()).tail(scope.local, GInt(1)).unfold(),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocalX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.mixed, 'a').by(Anon.unfold().values('name').fold()).tail(scope.local).unfold(),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXlimitXlocal_0XX_tailXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.mixed, 'a').by(Anon.limit(scope.local, GInt(0))).tail(scope.local, GInt(1)),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.mixed, 'a').by(Anon.unfold().values('name').fold()).tail(scope.local, GInt(2)),
  ],
  'g_VX1X_valuesXageX_tailXlocal_5X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').tail(scope.local, GInt(50)),
  ],
  'g_injectXlistX1_2_3XX_tailXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)]).tail(scope.local, GInt(1)),
  ],
  'g_VX1X_valueMapXnameX_tailXlocal_1X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).valueMap('name').tail(scope.local, GInt(1)),
  ],
  'g_injectX1_2_3X_tailXlocal_1X_unfold': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)]).tail(scope.local, GInt(1)).unfold(),
  ],
  'g_injectX1_2_3_4_5_6X_tailXlocal_1X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3)], [GInt(4), GInt(5), GInt(6)]).tail(scope.local, GInt(1)),
  ],
  'g_injectX1_2_3_4_5X_tailXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3), GInt(4), GInt(5)]).tail(scope.local, GInt(2)),
  ],
  'g_V_valuesXnameX_isXtypeOfXGType_STRINGXX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').is_(P.typeOf(gtype.STRING)),
  ],
  'g_V_valuesXnameX_isXtypeOfXjava_lang_StringXX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').is_(P.typeOf('String')),
  ],
  'g_V_hasXname_typeOfXGType_STRINGXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', P.typeOf(gtype.STRING)).values('name'),
  ],
  'g_V_orXhasXname_typeOfXGType_STRINGXX__hasXage_typeOfXGType_INTXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().or_(Anon.has('name', P.typeOf(gtype.STRING)), Anon.has('age', P.typeOf(gtype.INT))).values('name'),
  ],
  'g_V_andXhasXname_typeOfXGType_STRINGXX__hasXage_typeOfXGType_INTXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().and_(Anon.has('name', P.typeOf(gtype.STRING)), Anon.has('age', P.typeOf(gtype.INT))).values('name'),
  ],
  'g_V_notXhasXage_typeOfXGType_STRINGXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.has('age', P.typeOf(gtype.STRING))).values('name'),
  ],
  'g_V_valuesXageX_isXnotXtypeOfXGType_STRINGXXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.not_(P.typeOf(gtype.STRING))),
  ],
  'g_V_valuesXnameX_isXtypeOfXstringStringXX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').is_(P.typeOf('String')),
  ],
  'g_V_orXvaluesXageX_isXtypeOfXGType_INTXX__valuesXnameX_isXtypeOfXGType_STRINGXXX_count': <Function>[
    (GraphTraversalSource g) => g.V().or_(Anon.values('age').is_(P.typeOf(gtype.INT)), Anon.values('name').is_(P.typeOf(gtype.STRING))).count(),
  ],
  'g_V_whereXvaluesXnameX_isXtypeOfXGType_STRINGXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.values('name').is_(P.typeOf(gtype.STRING))).values('name'),
  ],
  'g_V_whereXvaluesXageX_isXtypeOfXGType_STRINGXXX_count': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.values('age').is_(P.typeOf(gtype.STRING))).count(),
  ],
  'g_V_whereXnotXvaluesXageX_isXtypeOfXGType_STRINGXXXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.not_(Anon.values('age').is_(P.typeOf(gtype.STRING)))).values('name'),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_NULLXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.NULL)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_BOOLEANXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.BOOLEAN)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_CHARXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.CHAR)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_BINARYXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.BINARY)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_UUIDXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.UUID)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_DATETIMEXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.DATETIME)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_DURATIONXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.DURATION)),
  ],
  'g_V_valuesXageX_isXtypeOfXnon_registered_NameXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf('non-registered-Name')),
  ],
  'g_injectXtrueX_isXtypeOfXGType_BOOLEANX': <Function>[
    (GraphTraversalSource g) => g.inject(true).is_(P.typeOf(gtype.BOOLEAN)),
  ],
  'g_V_path_isXtypeOfXGType_PATHXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').path().is_(P.typeOf(gtype.PATH)),
  ],
  'g_V_out_path_isXtypeOfXGType_PATHXX_count': <Function>[
    (GraphTraversalSource g) => g.V().out().path().is_(P.typeOf(gtype.PATH)).count(),
  ],
  'g_V_hasXname_markoX_out_out_path_isXtypeOfXGType_PATHXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').out().out().path().is_(P.typeOf(gtype.PATH)),
  ],
  'g_V_out_tree_isXtypeOfXGType_TREEXX_count': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').out().tree().is_(P.typeOf(gtype.TREE)).count(),
  ],
  'g_V_whereXtree_isXtypeOfXGType_TREEXXX_values_name': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.tree().is_(P.typeOf(gtype.TREE))).values('name'),
  ],
  'g_VX1X_outEXknowsX_subgraphXsgX_name_capXsgX_isXtypeOfXGType_GRAPHXX_count': <Function>[
    (GraphTraversalSource g) => g.V().outE('knows').subgraph('sg').cap('sg').is_(P.typeOf(gtype.GRAPH)).count(),
  ],
  'g_VX1X_outEXknowsX_subgraphXsgX_name_capXsgX_isX_notXtypeOfXGType_GRAPHXXX_count': <Function>[
    (GraphTraversalSource g) => g.V().outE('knows').subgraph('sg').cap('sg').is_(P.not_(P.typeOf(gtype.GRAPH))).count(),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_PATHXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.PATH)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_TREEXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.TREE)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_GRAPHXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.GRAPH)),
  ],
  'g_V_valuesXageX_isXtypeOfXGType_VPROPERTYXX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').is_(P.typeOf(gtype.VPROPERTY)),
  ],
  'g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').as_('a').out().in_().has('age').as_('b').select('a', 'b').where('a', P.eq('b')),
  ],
  'g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').as_('a').out().in_().has('age').as_('b').select('a', 'b').where('a', P.neq('b')),
  ],
  'g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').as_('a').out().in_().has('age').as_('b').select('a', 'b').where(Anon.as_('b').has('name', 'marko')),
  ],
  'g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').as_('a').out().in_().has('age').as_('b').select('a', 'b').where(Anon.as_('a').out('knows').as_('b')),
  ],
  'g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').where(Anon.as_('a').values('name').is_('josh')).in_('created').values('name'),
  ],
  'g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withSideEffect('a', ['josh', 'peter']).V(vid1).out('created').in_('created').values('name').where(P.within('a')),
  ],
  'g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').in_('created').as_('b').where('a', P.neq('b')).values('name'),
  ],
  'g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').in_('created').as_('b').where(Anon.as_('b').out('created').has('name', 'ripple')).values('age', 'name'),
  ],
  'g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').in_('created').where(P.eq('a')).values('name'),
  ],
  'g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').in_('created').where(P.neq('a')).values('name'),
  ],
  'g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().aggregate('x').out().where(P.not_(P.within('x'))),
  ],
  'g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().where(Anon.id().where(P.neq('a'))),
  ],
  'g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.bothE('created').where(P.without('e')).aggregate('e').otherV()).emit().path(),
  ],
  'g_V_whereXnotXoutXcreatedXXX_name': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.not_(Anon.out('created'))).values('name'),
  ],
  'g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').where(Anon.and_(Anon.as_('a').out('knows').as_('b'), Anon.or_(Anon.as_('b').out('created').has('name', 'ripple'), Anon.as_('b').in_('knows').count().is_(P.not_(P.eq(GInt(0))))))).select('a', 'b'),
  ],
  'g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.out('created').and_().out('knows').or_().in_('knows')).values('name'),
  ],
  'g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').as_('b').where(Anon.and_(Anon.as_('b').in_(), Anon.not_(Anon.as_('a').out('created').has('name', 'ripple')))).select('a', 'b'),
  ],
  'g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').as_('b').in_('created').as_('c').both('knows').both('knows').as_('d').where('c', P.not_(P.eq('a').or_(P.eq('d')))).select('a', 'b', 'c', 'd'),
  ],
  'g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').where(Anon.as_('b').in_().count().is_(P.eq(GInt(3))).or_().where(Anon.as_('b').out('created').and_().as_('b').has(t.label, 'person'))).select('a', 'b'),
  ],
  'g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').in_('created').as_('b').where('a', P.gt('b')).by('age').select('a', 'b').by('name'),
  ],
  'g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').outE('created').as_('b').inV().as_('c').where('a', P.gt('b').or_(P.eq('b'))).by('age').by('weight').by('weight').select('a', 'c').by('name'),
  ],
  'g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').outE('created').as_('b').inV().as_('c').in_('created').as_('d').where('a', P.lt('b').or_(P.gt('c')).and_(P.neq('d'))).by('age').by('weight').by(Anon.in_('created').values('age').min()).select('a', 'c', 'd').by('name'),
  ],
  'g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out().has('age').where(P.gt('a')).by('age').values('name'),
  ],
  'g_VX3X_asXaX_in_out_asXbX_whereXa_eqXbXX_byXageX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.V(vid3).as_('a').in_().out().as_('b').where('a', P.eq('b')).by('age').values('name'),
  ],
  'g_withStrategiesXProductiveByStrategyX_VX3X_asXaX_in_out_asXbX_whereXa_eqXbXX_byXageX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid3}) => g.withStrategies(ProductiveByStrategy()).V(vid3).as_('a').in_().out().as_('b').where('a', P.eq('b')).by('age').values('name'),
  ],
  'g_V_asXnX_whereXorXhasLabelXsoftwareX_hasLabelXpersonXXX_selectXnX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('n').where(Anon.or_(Anon.hasLabel('software'), Anon.hasLabel('person'))).select('n').by('name'),
  ],
  'g_V_asXnX_whereXorXselectXnX_hasLabelXsoftwareX_selectXnX_hasLabelXpersonXXX_selectXnX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('n').where(Anon.or_(Anon.select('n').hasLabel('software'), Anon.select('n').hasLabel('person'))).select('n').by('name'),
  ],
  'g_V_hasLabelXpersonX_asXxX_whereXinEXknowsX_count_isXgteX1XXX_selectXxX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('x').where(Anon.inE('knows').count().is_(P.gte(GInt(1)))).select('x'),
  ],
  'get_g_V_whereXage_isXgt_30XX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.values('age').is_(P.gt(GInt(30)))),
  ],
  'g_V_whereXlabel_isXsoftwareXX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.label().is_('software')),
  ],
  'g_V_whereXlabel_isXpersonXX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.label().is_('person')),
  ],
  'g_withStrategiesXAdjacentToIncidentStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(AdjacentToIncidentStrategy()).V(),
  ],
  'g_withoutStrategiesXAdjacentToIncidentStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(AdjacentToIncidentStrategy).V(),
  ],
  'g_withStrategiesXAdjacentToIncidentStrategyX_V_out_count': <Function>[
    (GraphTraversalSource g) => g.withStrategies(AdjacentToIncidentStrategy()).V().out().count(),
  ],
  'g_withStrategiesXAdjacentToIncidentStrategyX_V_whereXoutX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(AdjacentToIncidentStrategy()).V().where(Anon.out()),
  ],
  'g_withStrategiesXByModulatorOptimizationStrategyX_V_order_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ByModulatorOptimizationStrategy()).V().order().by(Anon.values('name')),
  ],
  'g_withoutStrategiesXByModulatorOptimizationStrategyX_V_order_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ByModulatorOptimizationStrategy).V().order().by(Anon.values('name')),
  ],
  'g_withStrategiesXComputerFinalizationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ComputerFinalizationStrategy()).V(),
  ],
  'g_withoutStrategiesXByModulatorOptimizationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ComputerFinalizationStrategy).V(),
  ],
  'g_withStrategiesXComputerVerificationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ComputerVerificationStrategy()).V(),
  ],
  'g_withoutStrategiesXComputerVerificationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ComputerVerificationStrategy).V(),
  ],
  'g_withStrategiesXConnectiveStrategyStrategyX_V_hasXname_markoX_or_whereXinXknowsX_hasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ConnectiveStrategy()).V().has('name', 'marko').or_().where(Anon.in_('knows').has('name', 'marko')),
  ],
  'g_withoutStrategiesXConnectiveStrategyX_V_hasXname_markoX_or_whereXinXknowsX_hasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ConnectiveStrategy).V().has('name', 'marko').or_().where(Anon.in_('knows').has('name', 'marko')),
  ],
  'g_withStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(CountStrategy()).V().where(Anon.outE().count().is_(GInt(0))),
  ],
  'g_withoutStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(CountStrategy).V().where(Anon.outE().count().is_(GInt(0))),
  ],
  'g_withStrategiesXEarlyLimitStrategyX_V_out_order_byXnameX_valueMap_limitX3X_selectXnameX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(EarlyLimitStrategy()).V().out().order().by('name').valueMap().limit(GInt(3)).select('name'),
  ],
  'g_withoutStrategiesXEarlyLimitStrategyX_V_out_order_byXnameX_valueMap_limitX3X_selectXnameX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(EarlyLimitStrategy).V().out().order().by('name').valueMap().limit(GInt(3)).select('name'),
  ],
  'g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_true_logWarning_falseXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(EdgeLabelVerificationStrategy(throwException: true, logWarning: false)).V().out(),
  ],
  'g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_false_logWarning_falseXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(EdgeLabelVerificationStrategy(throwException: false, logWarning: false)).V().out(),
  ],
  'g_withoutStrategiesXEdgeLabelVerificationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(EdgeLabelVerificationStrategy).V().out(),
  ],
  'g_withStrategiesXElementIdStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ElementIdStrategy()).V(),
  ],
  'g_withoutStrategiesXElementIdStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ElementIdStrategy).V(),
  ],
  'g_withStrategiesXFilterRankingStrategyX_V_out_order_dedup': <Function>[
    (GraphTraversalSource g) => g.withStrategies(FilterRankingStrategy()).V().out().order().dedup(),
  ],
  'g_withoutStrategiesXFilterRankingStrategyX_V_out_order_dedup': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(FilterRankingStrategy).V().out().order().dedup(),
  ],
  'g_withStrategiesXGraphFilterStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(GraphFilterStrategy()).V(),
  ],
  'g_withoutStrategiesXGraphFilterStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(GraphFilterStrategy).V(),
  ],
  'g_withStrategiesXHaltedTraverserStrategyXDetachedFactoryXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(HaltedTraverserStrategy(haltedTraverserFactory: 'org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory')).V(),
  ],
  'g_withStrategiesXHaltedTraverserStrategyXReferenceFactoryXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(HaltedTraverserStrategy(haltedTraverserFactory: 'org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory')).V(),
  ],
  'g_withoutStrategiesXHaltedTraverserStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(HaltedTraverserStrategy).V(),
  ],
  'g_withStrategiesXIdentityRemovalStrategyX_V_identity_out': <Function>[
    (GraphTraversalSource g) => g.withStrategies(IdentityRemovalStrategy()).V().identity().out(),
  ],
  'g_withoutStrategiesXIdentityRemovalStrategyX_V_identity_out': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(IdentityRemovalStrategy).V().identity().out(),
  ],
  'g_withStrategiesXIncidentToAdjacentStrategyX_V_outE_inV': <Function>[
    (GraphTraversalSource g) => g.withStrategies(IncidentToAdjacentStrategy()).V().outE().inV(),
  ],
  'g_withoutStrategiesXIncidentToAdjacentStrategyX_V_outE_inV': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(IncidentToAdjacentStrategy).V().outE().inV(),
  ],
  'g_withStrategiesXInlineFilterStrategyX_V_filterXhasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(InlineFilterStrategy()).V().filter_(Anon.has('name', 'marko')),
  ],
  'g_withoutStrategiesXInlineFilterStrategyX_V_filterXhasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(InlineFilterStrategy).V().filter_(Anon.has('name', 'marko')),
  ],
  'g_withStrategiesXLambdaRestrictionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(LambdaRestrictionStrategy()).V(),
  ],
  'g_withoutStrategiesXLambdaRestrictionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(LambdaRestrictionStrategy).V(),
  ],
  'g_withStrategiesXLazyBarrierStrategyX_V_out_bothE_count': <Function>[
    (GraphTraversalSource g) => g.withStrategies(LazyBarrierStrategy()).V().out().bothE().count(),
  ],
  'g_withoutStrategiesXLazyBarrierStrategyX_V_out_bothE_count': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(LazyBarrierStrategy).V().out().bothE().count(),
  ],
  'g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_CountMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(MatchAlgorithmStrategy(matchAlgorithm: 'org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep\$CountMatchAlgorithm')).V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('a').out('created').as_('c')),
  ],
  'g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_GreedyMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(MatchAlgorithmStrategy(matchAlgorithm: 'org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep\$GreedyMatchAlgorithm')).V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('a').out('created').as_('c')),
  ],
  'g_withoutStrategiesXMatchAlgorithmStrategyX_V_matchXa_knows_b__a_created_cX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(MatchAlgorithmStrategy).V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('a').out('created').as_('c')),
  ],
  'g_withStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(MatchPredicateStrategy()).V().match_(Anon.as_('a').out('created').has('name', 'lop').as_('b'), Anon.as_('b').in_('created').has('age', GInt(29)).as_('c')).where(Anon.as_('c').repeat(Anon.out()).times(GInt(2))).select('a', 'b', 'c'),
  ],
  'g_withoutStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(MatchPredicateStrategy).V().match_(Anon.as_('a').out('created').has('name', 'lop').as_('b'), Anon.as_('b').in_('created').has('age', GInt(29)).as_('c')).where(Anon.as_('c').repeat(Anon.out()).times(GInt(2))).select('a', 'b', 'c'),
  ],
  'g_withStrategiesXMessagePassingReductionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(MessagePassingReductionStrategy()).V(),
  ],
  'g_withoutStrategiesXMessagePassingReductionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(MessagePassingReductionStrategy).V(),
  ],
  'g_V_coworker': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').filter_(Anon.outE('created')).aggregate('p').as_('p1').values('name').as_('p1n').select('p').unfold().where(P.neq('p1')).as_('p2').values('name').as_('p2n').select('p2').out('created').choose(Anon.in_('created').where(P.eq('p1')), Anon.values('name'), Anon.constant([])).group().by(Anon.select('p1n')).by(Anon.group().by(Anon.select('p2n')).by(Anon.unfold().fold().project('numCoCreated', 'coCreated').by(Anon.count(scope.local)).by())).unfold(),
  ],
  'g_V_coworker_with_midV': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').filter_(Anon.outE('created')).as_('p1').V().hasLabel('person').where(P.neq('p1')).filter_(Anon.outE('created')).as_('p2').map_(Anon.out('created').where(Anon.in_('created').as_('p1')).values('name').fold()).group().by(Anon.select('p1').by('name')).by(Anon.group().by(Anon.select('p2').by('name')).by(Anon.project('numCoCreated', 'coCreated').by(Anon.count(scope.local)).by())).unfold(),
  ],
  'g_withoutStrategiesXOptionsStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(OptionsStrategy).V(),
  ],
  'g_withStrategiesXOrderLimitStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(OrderLimitStrategy()).V(),
  ],
  'g_withoutStrategiesXOrderLimitStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(OrderLimitStrategy).V(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'b').property('name', 'bob'),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).V().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'b').property('name', 'bob'),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a', 'b'])).V().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'b').property('name', 'bob'),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['c'])).V().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_bothE_weight': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).V().bothE().values('weight'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_bothE_weight': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['b'])).V().bothE().values('weight'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_bothE_dedup_weight': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a', 'b'])).V().bothE().dedup().values('weight'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_bothE_weight': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['c'])).V().bothE().values('weight'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_both_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).V().both().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_both_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['b'])).V().both().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_both_dedup_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a', 'b'])).V().both().dedup().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_both_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['c'])).V().both().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_out_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).V().out().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_in_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['b'])).V().in_().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_out_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a', 'b'])).V().out().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_out_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('_partition', 'a').property('name', 'alice').as_('a').addV('person').property('_partition', 'b').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('_partition', 'a').property('weight', GDouble(1.0)).addE('knows').from_('b').to('a').property('_partition', 'b').property('weight', GDouble(2.0)),
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['c'])).V().out().values('name'),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_addVXpersonX_propertyXname_aliceX_addXselfX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).addV('person').property('name', 'alice').addE('self'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('_partition', 'a'),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E().has('_partition', 'a'),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectXzeroX_addVXpersonX_propertyXname_aliceX_addXselfX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).inject(GInt(0)).addV('person').property('name', 'alice').addE('self'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('_partition', 'a'),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E().has('_partition', 'a'),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeV': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'alice').has('_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0X_mergeV': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).inject(GInt(0)).mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'alice').has('_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeE': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'a').property('name', 'bob'),
    (GraphTraversalSource g, {dynamic xx1}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().has('knows', '_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0XmergeE': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'a').property('name', 'bob'),
    (GraphTraversalSource g, {dynamic xx1}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).inject(GInt(0)).mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().has('knows', '_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeVXlabel_person_name_aliceX_optionXonMatch_name_bobX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('_partition', 'a').property('name', 'alice').addV('person').property('_partition', 'b').property('name', 'alice'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).mergeV(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'bob').has('_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeV_optionXonCreateX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('_partition', 'b').property('name', 'alice'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('name', 'alice').has('age', GInt(35)).has('_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
  ],
  'g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0X__mergeV_optionXonCreateX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('_partition', 'b').property('name', 'alice'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.withStrategies(PartitionStrategy(partitionKey: '_partition', writePartition: 'a', readPartitions: ['a'])).inject(GInt(0)).mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('name', 'alice').has('age', GInt(35)).has('_partition', 'a'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
  ],
  'g_withStrategiesXPathProcessorStrategyX_V_asXaX_selectXaX_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(PathProcessorStrategy()).V().as_('a').select('a').by(Anon.values('name')),
  ],
  'g_withoutStrategiesXPathProcessorStrategyX_V_asXaX_selectXaX_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(PathProcessorStrategy).V().as_('a').select('a').by(Anon.values('name')),
  ],
  'g_withStrategiesXPathRetractionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(PathRetractionStrategy()).V(),
  ],
  'g_withoutStrategiesXPathRetractionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(PathRetractionStrategy).V(),
  ],
  'g_V_shortestpath': <Function>[
    (GraphTraversalSource g) => g.V().as_('v').both().as_('v').project('src', 'tgt', 'p').by(Anon.select(pop.first, 'v')).by(Anon.select(pop.last, 'v')).by(Anon.select(pop.all, 'v')).as_('triple').group('x').by(Anon.select('src', 'tgt')).by(Anon.select('p').fold()).select('tgt').barrier().repeat(Anon.both().as_('v').project('src', 'tgt', 'p').by(Anon.select(pop.first, 'v')).by(Anon.select(pop.last, 'v')).by(Anon.select(pop.all, 'v')).as_('t').filter_(Anon.select(pop.all, 'p').count(scope.local).as_('l').select(pop.last, 't').select(pop.all, 'p').dedup(scope.local).count(scope.local).where(P.eq('l'))).where('src', P.neq('tgt')).select(pop.last, 't').not_(Anon.select(pop.all, 'p').as_('p').count(scope.local).as_('l').select(pop.all, 'x').unfold().filter_(Anon.select(column.keys).where(P.eq('t')).by(Anon.select('src', 'tgt'))).filter_(Anon.select(column.values).unfold().or_(Anon.count(scope.local).where(P.lt('l')), Anon.where(P.eq('p'))))).barrier().group('x').by(Anon.select('src', 'tgt')).by(Anon.select(pop.all, 'p').fold()).select('tgt').barrier()).cap('x').select(column.values).unfold().unfold().map_(Anon.unfold().values('name').fold()),
  ],
  'g_V_playlist_paths': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SeedStrategy(seed: GInt(99999))).V().has('name', 'Bob_Dylan').in_('sungBy').as_('a').repeat(Anon.out().order().by(order.shuffle).simplePath().from_('a')).until(Anon.out('writtenBy').has('name', 'Johnny_Cash')).limit(GInt(1)).as_('b').repeat(Anon.out().order().by(order.shuffle).as_('c').simplePath().from_('b').to('c')).until(Anon.out('sungBy').has('name', 'Grateful_Dead')).limit(GInt(1)).path().from_('a').unfold().project('song', 'artists').by('name').by(Anon.coalesce(Anon.out('sungBy', 'writtenBy').dedup().values('name'), Anon.constant('Unknown')).fold()),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_group_byXageX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().group().by('age').by('name'),
  ],
  'g_withoutStrategiesXProductiveByStrategyX_V_group_byXageX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ProductiveByStrategy).V().group().by('age').by('name'),
  ],
  'g_withStrategiesXProfileStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProfileStrategy()).V(),
  ],
  'g_withoutStrategiesXProfileStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ProfileStrategy).V(),
  ],
  'g_withStrategiesXReadOnlyStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReadOnlyStrategy()).V(),
  ],
  'g_withStrategiesXReadOnlyStrategyX_V_outXknowsX_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReadOnlyStrategy()).V().out('knows').values('name'),
  ],
  'g_withStrategiesXReadOnlyStrategyX_addVXpersonX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReadOnlyStrategy()).addV('person'),
  ],
  'g_withStrategiesXReadOnlyStrategyX_addVXpersonX_fromXVX1XX_toXVX2XX': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.withStrategies(ReadOnlyStrategy()).addE('link').from_(Anon.V(vid1)).to(Anon.V(vid2)),
  ],
  'g_withStrategiesXReadOnlyStrategyX_V_addVXpersonX_fromXVX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withStrategies(ReadOnlyStrategy()).V().addE('link').from_(Anon.V(vid1)),
  ],
  'g_withStrategiesXReadOnlyStrategyX_V_propertyXname_joshX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReadOnlyStrategy()).V().property('name', 'josh'),
  ],
  'g_withStrategiesXReadOnlyStrategyX_E_propertyXweight_0X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReadOnlyStrategy()).E().property('weight', GInt(0)),
  ],
  'g_V_classic_recommendation': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'DARK STAR').as_('a').out('followedBy').aggregate('stash').in_('followedBy').where(P.neq('a').and_(P.not_(P.within('stash')))).groupCount().unfold().project('x', 'y', 'z').by(Anon.select(column.keys).values('name')).by(Anon.select(column.keys).values('performances')).by(Anon.select(column.values)).order().by(Anon.select('z'), order.desc).by(Anon.select('y'), order.asc).limit(GInt(5)).local(Anon.aggregate('m')).select('x'),
  ],
  'g_V_classic_recommendation_ranked': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'DARK STAR').as_('a').out('followedBy').aggregate('stash').in_('followedBy').where(P.neq('a').and_(P.not_(P.within('stash')))).groupCount().unfold().project('x', 'y', 'z').by(Anon.select(column.keys).values('name')).by(Anon.select(column.keys).values('performances')).by(Anon.select(column.values)).order().by(Anon.select('z'), order.desc).by(Anon.select('y'), order.asc).limit(GInt(5)).local(Anon.aggregate('m')),
  ],
  'g_withStrategiesXReferenceElementStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReferenceElementStrategy()).V(),
  ],
  'g_withoutStrategiesXReferenceElementStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ReferenceElementStrategy).V(),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.out()).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.out()).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXinX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.in_()).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXinX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.in_()).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXout_hasXname_notStartingWithXzXXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.out().has('name', TextP.notStartingWith('z'))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXout_hasXname_notStartingWithXzXXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.out().has('name', TextP.notStartingWith('z'))).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXin_hasXage_gtX20XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.in_().has('age', P.gt(GInt(20)))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXin_hasXage_gtX20XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.in_().has('age', P.gt(GInt(20)))).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXboth_hasXage_ltX30XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.both().has('age', P.lt(GInt(30)))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXboth_hasXage_ltX30XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.both().has('age', P.lt(GInt(30)))).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXbothE_otherV_hasXage_ltX30XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.bothE().otherV().has('age', P.lt(GInt(30)))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXbothE_otherV_hasXage_ltX30XXX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.bothE().otherV().has('age', P.lt(GInt(30)))).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXboth_limitX1XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.both().limit(GInt(1))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXboth_limitX1XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.both().limit(GInt(1))).times(GInt(2)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_order_byXnameX_repeatXboth_order_byXnameX_aggregateXxXX_timesX2X_limitX10X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().order().by('name').repeat(Anon.both().order().by('name').aggregate('x')).times(GInt(2)).limit(GInt(10)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_order_byXnameX_repeatXboth_order_byXnameX_aggregateXxXX_timesX2X_limitX10X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().order().by('name').repeat(Anon.both().order().by('name').aggregate('x')).times(GInt(2)).limit(GInt(10)),
  ],
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXboth_sampleX1XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withStrategies(RepeatUnrollStrategy()).V().repeat(Anon.both().sample(GInt(1))).times(GInt(2)),
  ],
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXboth_sampleX1XX_timesX2X': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(RepeatUnrollStrategy).V().repeat(Anon.both().sample(GInt(1))).times(GInt(2)),
  ],
  'g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXid_123X_propertyXname_markoX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReservedKeysVerificationStrategy(throwException: true)).addV('person').property('id', GInt(123)).property('name', 'marko'),
  ],
  'g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXage_29X_propertyXname_markoX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ReservedKeysVerificationStrategy(throwException: true, keys: <dynamic>{'age'})).addV('person').property('age', GInt(29)).property('name', 'marko'),
  ],
  'g_withoutStrategiesXReservedKeysVerificationStrategyX_addVXpersonX_propertyXid_123X_propertyXname_markoX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(ReservedKeysVerificationStrategy).addV('person').property('id', GInt(123)).property('name', 'marko').values(),
  ],
  'g_withoutStrategiesXSeedStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(SeedStrategy).V(),
  ],
  'g_withStrategiesXStandardVerificationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(StandardVerificationStrategy()).V(),
  ],
  'g_withoutStrategiesXStandardVerificationStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(StandardVerificationStrategy).V(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).E(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_inE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).inE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).out(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_in': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).in_(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_both': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).both(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_bothE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).bothE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_localXbothE_limitX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).V(vid4).local(Anon.bothE().limit(GInt(1))),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_EX11X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).E(eid11).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphAXX_EX12X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid12}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')))).E(eid12).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid1).outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid1).out(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_outXcreatedX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid1).out('knows'),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outXcreatedX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).out('created'),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).out(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_bothE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).bothE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_both': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).both(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outV_outE': <Function>[
    (GraphTraversalSource g, {dynamic eid8}) => g.withStrategies(SubgraphStrategy(edges: Anon.or_(Anon.has('weight', GDouble(1.0)).hasLabel('knows'), Anon.has('weight', GDouble(0.4)).hasLabel('created').outV().has('name', 'marko'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid8).outV().outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_inXknowsX_hasXname_markoXXX_V_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.in_('knows').has('name', 'marko'))).V().values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_in_hasXname_markoXXX_V_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.in_().has('name', 'marko'))).V().values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_inXknowsX_whereXoutXcreatedX_hasXname_lopXXXX_V_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.in_('knows').where(Anon.out('created').has('name', 'lop')))).V().values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_in_hasXname_markoX_outXcreatedX_hasXname_lopXXXX_V_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.in_().where(Anon.has('name', 'marko').out('created').has('name', 'lop')))).V().values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_orXboth_hasXname_markoX_hasXname_markoXXXX_V_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.or_(Anon.both().has('name', 'marko'), Anon.has('name', 'marko')))).V().where(Anon.bothE().count().is_(P.neq(GInt(0)))).values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_inE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).inE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).out(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_in': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).in_(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_both': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).both(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_bothE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).bothE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_localXbothE_limitX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).local(Anon.bothE().limit(GInt(1))),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX11X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid11).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX12X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid12}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid12).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX9X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid9}) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid9).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_hasXname_withinXripple_josh_markoXXX_V_asXaX_out_in_asXbX_dedupXa_bX_name': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertices: Anon.has('name', P.within('ripple', 'josh', 'marko')))).V().as_('a').out().in_().as_('b').dedup('a', 'b').values('name'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_propertiesXlocationX_value': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))))).V().properties('location').value_(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_valuesXlocationX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))))).V().values('location'),
  ],
  'g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_asXaX_propertiesXlocationX_asXbX_selectXaX_outE_properties_selectXbX_value_dedup': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))))).V().as_('a').properties('location').as_('b').select('a').outE().properties().select('b').value_().dedup(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_asXaX_valuesXlocationX_asXbX_selectXaX_outE_properties_selectXbX_dedup': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))))).V().as_('a').values('location').as_('b').select('a').outE().properties().select('b').dedup(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_hasXname_neqXstephenXX_vertexProperties_hasXstartTime_gtX2005XXXX_V_propertiesXlocationX_value': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))), vertices: Anon.has('name', P.neq('stephen')))).V().properties('location').value_(),
  ],
  'g_withStrategiesXSubgraphStrategyXvertices_hasXname_neqXstephenXX_vertexProperties_hasXstartTime_gtX2005XXXX_V_valuesXlocationX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(vertexProperties: Anon.has('startTime', P.gt(GInt(2005))), vertices: Anon.has('name', P.neq('stephen')))).V().values('location'),
  ],
  'g_withStrategiesXSubgraphStrategyXedges_hasLabelXusesX_hasXskill_5XXX_V_outE_valueMap_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(edges: Anon.hasLabel('uses').has('skill', GInt(5)))).V().outE().valueMap().select(column.values).unfold(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(),
  ],
  'g_withStrategiesXSubgraphStrategyXcheckAdjacentVertices_subgraphDXX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: true, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).outE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_inE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).inE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).out(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_in': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).in_(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_both': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).both(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_bothE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).bothE(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_localXbothE_limitX1XX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).V(vid4).local(Anon.bothE().limit(GInt(1))),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX11X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid11).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX12X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid12}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid12).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX9X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid9}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: false, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid9).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXcheckAdjacentVertices_subgraphDXX_EX9X_bothV': <Function>[
    (GraphTraversalSource g, {dynamic eid9}) => g.withStrategies(SubgraphStrategy(checkAdjacentVertices: true, vertices: Anon.has('name', P.within('josh', 'lop', 'ripple')), edges: Anon.or_(Anon.has('weight', GDouble(0.4)).hasLabel('created'), Anon.has('weight', GDouble(1.0)).hasLabel('created')))).E(eid9).bothV(),
  ],
  'g_withStrategiesXSubgraphStrategyXuseMapStepsInFilterX_E': <Function>[
    (GraphTraversalSource g) => g.withStrategies(SubgraphStrategy(edges: Anon.label().is_(P.eq('created')), vertices: Anon.values('name').is_(P.within('lop', 'josh')), checkAdjacentVertices: true)).E(),
  ],
  'g_withStrategiesXVertexProgramRestrictionStrategyX_withoutStrategiesXVertexProgramStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(VertexProgramRestrictionStrategy()).withoutStrategies(VertexProgramStrategy).V(),
  ],
  'g_withStrategiesXVertexProgramRestrictionStrategy_VertexProgramStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(VertexProgramRestrictionStrategy(), VertexProgramStrategy()).V(),
  ],
  'g_withoutStrategiesXVertexProgramRestrictionStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(VertexProgramRestrictionStrategy).withStrategies(VertexProgramStrategy()).V(),
  ],
  'g_withStrategiesXVertexProgramStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withStrategies(VertexProgramStrategy()).V(),
  ],
  'g_withoutStrategiesXVertexProgramStrategyX_V': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(VertexProgramStrategy).V(),
  ],
  'g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').addE('createdBy').to('a'),
    (GraphTraversalSource g, {dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).inE(),
  ],
  'g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('created').addE('createdBy').to('a').property('weight', GDouble(2.0)),
    (GraphTraversalSource g, {dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).inE().has('weight', GDouble(2.0)),
  ],
  'g_V_outE_propertyXweight_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().outE().property('weight', null),
    (GraphTraversalSource g) => g.E().properties('weight'),
  ],
  'g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX': <Function>[
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V().aggregate('x').as_('a').select('x').unfold().addE('existsWith').to('a').property('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).bothE('existsWith').has('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).bothE('existsWith').has('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).bothE('existsWith').has('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).bothE('existsWith').has('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).bothE('existsWith').has('time', 'now'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).inE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).outE('existsWith'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).bothE('existsWith').has('time', 'now'),
  ],
  'g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X': <Function>[
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V().as_('a').out('created').in_('created').where(P.neq('a')).as_('b').addE('codeveloper').from_('a').to('b').property('year', GInt(2009)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid1).inE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid1).outE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid1).bothE('codeveloper').has('year', GInt(2009)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid2).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid4).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid4).inE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid4).outE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid4).bothE('codeveloper').has('year', GInt(2009)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid6).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid6).inE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid6).outE('codeveloper'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid4, dynamic vid6}) => g.V(vid6).bothE('codeveloper').has('year', GInt(2009)),
  ],
  'g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX': <Function>[
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V().as_('a').in_('created').addE('createdBy').from_('a').property('year', GInt(2009)).property('acl', 'public'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).inE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).outE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).bothE('createdBy').has('year', GInt(2009)).has('acl', 'public'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).inE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).outE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).bothE('createdBy').has('year', GInt(2009)).has('acl', 'public'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).inE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).outE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).bothE('createdBy').has('year', GInt(2009)).has('acl', 'public'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).inE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).outE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).bothE('createdBy').has('year', GInt(2009)).has('acl', 'public'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).inE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).outE('createdBy'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).bothE('createdBy').has('year', GInt(2009)).has('acl', 'public'),
  ],
  'g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X': <Function>[
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid1).addE('knows').to('b').property('weight', GDouble(0.5)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid1).inE('knows'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid1).outE('knows'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid1).bothE('knows').has('weight', GDouble(0.5)),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid6).bothE(),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid6).inE('knows'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid6).outE('knows'),
    (GraphTraversalSource g, {dynamic vid1, dynamic vid6}) => g.V(vid6).bothE('knows').has('weight', GDouble(0.5)),
  ],
  'g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX': <Function>[
    (GraphTraversalSource g) => g.addV().as_('first').repeat(Anon.addE('next').to(Anon.addV()).inV()).times(GInt(5)).addE('next').to(Anon.select('first')),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.E().hasLabel('next'),
    (GraphTraversalSource g) => g.V().limit(GInt(1)).bothE(),
    (GraphTraversalSource g) => g.V().limit(GInt(1)).inE(),
    (GraphTraversalSource g) => g.V().limit(GInt(1)).outE(),
  ],
  'g_V_hasXname_markoX_asXaX_outEXcreatedX_asXbX_inV_addEXselectXbX_labelX_toXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.V().has('name', 'marko').as_('a').outE('created').as_('b').inV().addE(Anon.select('b').label()).to('a'),
    (GraphTraversalSource g, {dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).bothE(),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).inE('created'),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).in_('created').has('name', 'lop'),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('created'),
  ],
  'g_addEXV_outE_label_groupCount_orderXlocalX_byXvalues_descX_selectXkeysX_unfold_limitX1XX_fromXV_hasXname_vadasXX_toXV_hasXname_lopXX': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid2}) => g.addE(Anon.V().outE().label().groupCount().order(scope.local).by(column.values, order.desc).select(column.keys).unfold().limit(GInt(1))).from_(Anon.V().has('name', 'vadas')).to(Anon.V().has('name', 'lop')),
    (GraphTraversalSource g, {dynamic vid2}) => g.E(),
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).bothE(),
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).inE('knows'),
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).outE('created'),
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).out('created').has('name', 'lop'),
  ],
  'g_addEXknowsX_fromXVXvid1XX_toXVXvid6XX_propertyXweight_0_1X': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.addE('knows').from_(Anon.V(vid1)).to(Anon.V(vid6)).property('weight', xx1),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.V(vid1).outE('knows'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.V(vid1).out('knows').has('name', 'peter'),
  ],
  'g_addEXknowsvarX_fromXVXvid1XX_toXVXvid6XX_propertyXweight_0_1X': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic xx2, dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic xx2, dynamic vid1}) => g.addE(xx1).from_(Anon.V(vid1)).to(Anon.V(vid6)).property('weight', xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic xx2, dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic xx2, dynamic vid1}) => g.V(vid1).outE('knows'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic xx2, dynamic vid1}) => g.V(vid1).out('knows').has('name', 'peter'),
  ],
  'g_VXaX_addEXknowsX_toXbX_propertyXweight_0_1X': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.V(vid1).addE('knows').to(Anon.V(vid6)).property('weight', xx1),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.V(vid1).outE('knows'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid6, dynamic vid1}) => g.V(vid1).out('knows').has('name', 'peter'),
  ],
  'g_addEXknowsXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g) => g.addE('knows').property('weight', null).from_(Anon.V().has('name', 'marko')).to(Anon.V().has('name', 'vadas')),
    (GraphTraversalSource g) => g.E().has('knows', 'weight', null),
  ],
  'g_addEXknowsvarXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g, {dynamic xx1}) => g.addE(xx1).property('weight', null).from_(Anon.V().has('name', 'marko')).to(Anon.V().has('name', 'vadas')),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().has('knows', 'weight', null),
  ],
  'g_unionXaddEXknowsvarXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g, {dynamic xx1}) => g.union(Anon.addE(xx1).property('weight', GInt(1)).from_(Anon.V().has('name', 'marko')).to(Anon.V().has('name', 'vadas'))),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().has('knows', 'weight', GInt(1)),
  ],
  'g_addEXedgeX_fromXV_hasXname_markoXX_toXV_hasXname_vadasXX_propertyXweight_0_5X_withXkey_valueX_valuesXweight_keyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g) => g.addE('edge').from_(Anon.V().has('name', 'marko')).to(Anon.V().has('name', 'vadas')).property('weight', GDouble(0.5)).with_('key', 'value').values('weight', 'key'),
  ],
  'g_addEXknowsX_fromXV_hasXname_markoXX_toXV_hasXname_vadasXX_propertyXweight_0_5X_addEXknowsX_fromXV_hasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g) => g.addE('knows').from_(Anon.V().has('name', 'marko')).to(Anon.V().has('name', 'vadas')).property('weight', GDouble(0.5)).addE('knows').from_(Anon.V().has('name', 'marko')),
  ],
  'g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').addV('animal').property('age', Anon.select('a').by('age')).property('name', 'puppy'),
    (GraphTraversalSource g, {dynamic vid1}) => g.V().has('animal', 'age', GInt(29)),
  ],
  'g_V_addVXanimalX_propertyXage_0X': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().addV('animal').property('age', GInt(0)),
    (GraphTraversalSource g) => g.V().has('animal', 'age', GInt(0)),
  ],
  'g_V_addVXanimalvarX_propertyXage_0varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().addV(xx1).property('age', xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('animal', 'age', GInt(0)),
  ],
  'g_addVXpersonX_propertyXname_stephenX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV('person').property('name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_addVXpersonvarX_propertyXname_stephenvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV(xx1).property('name', xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_V_hasLabelXpersonX_propertyXname_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().hasLabel('person').property(cardinality.single, 'name', null),
    (GraphTraversalSource g) => g.V().properties('name'),
  ],
  'g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV('person').property(cardinality.single, 'name', 'stephen').property(cardinality.single, 'name', 'stephenm'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephenm'),
  ],
  'get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV('person').property(cardinality.single, 'name', 'stephen').property(cardinality.single, 'name', 'stephenm', 'since', GInt(2010)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephenm'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephenm').properties('name').has('since', GInt(2010)),
  ],
  'g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().has('name', 'marko').property('friendWeight', Anon.outE('knows').values('weight').sum(), 'acl', 'private'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('friendWeight', GDouble(1.5)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('friendWeight').has('acl', 'private'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('friendWeight').count(),
  ],
  'g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV('animal').property('name', 'mateo').property('name', 'gateo').property('name', 'cateo').property('age', GInt(5)),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'mateo').has('name', 'gateo').has('name', 'cateo').has('age', GInt(5)),
  ],
  'g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.withSideEffect('a', 'marko').addV().property('name', Anon.select('a')).values('name'),
    (GraphTraversalSource g) => g.V().has('name', 'marko'),
  ],
  'g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV('person').property(cardinality.single, 'name', 'stephen').property(cardinality.single, 'name', 'stephenm', 'since', GInt(2010)),
    (GraphTraversalSource g) => g.V().has('name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('name', 'stephenm'),
    (GraphTraversalSource g) => g.V().has('name', 'stephenm').properties('name').has('since', GInt(2010)),
  ],
  'g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().addV('animal').property('name', Anon.values('name')).property('name', 'an animal').property(Anon.values('name'), Anon.label()),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'marko').has('name', 'an animal').has('marko', 'person'),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'vadas').has('name', 'an animal').has('vadas', 'person'),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'lop').has('name', 'an animal').has('lop', 'software'),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'josh').has('name', 'an animal').has('josh', 'person'),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'ripple').has('name', 'an animal').has('ripple', 'software'),
    (GraphTraversalSource g) => g.V().hasLabel('animal').has('name', 'peter').has('name', 'an animal').has('peter', 'person'),
  ],
  'g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.withSideEffect('a', 'test').V().hasLabel('software').property('temp', Anon.select('a')).valueMap('name', 'temp'),
  ],
  'g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.withSideEffect('a', 'name').addV().property(Anon.select('a'), 'marko').values('name'),
    (GraphTraversalSource g) => g.V().has('name', 'marko'),
  ],
  'g_V_asXaX_hasXname_markoX_outXcreatedX_asXbX_addVXselectXaX_labelX_propertyXtest_selectXbX_labelX_valueMap_withXtokensX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().as_('a').has('name', 'marko').out('created').as_('b').addV(Anon.select('a').label()).property('test', Anon.select('b').label()).valueMap().with_(WithOptions.tokens),
    (GraphTraversalSource g) => g.V().has('person', 'test', 'software'),
  ],
  'g_addVXV_hasXname_markoX_propertiesXnameX_keyX_label': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.addV(Anon.V().has('name', 'marko').properties('name').key_()).label(),
  ],
  'g_addV_propertyXlabel_personX': <Function>[
    (GraphTraversalSource g) => g.addV().property(t.label, 'person'),
    (GraphTraversalSource g) => g.V().hasLabel('person'),
  ],
  'g_addV_propertyXlabel_personvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV().property(t.label, xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasLabel('person'),
  ],
  'g_addV_propertyXid_1X': <Function>[
    (GraphTraversalSource g) => g.addV().property(t.id, GInt(1)),
    (GraphTraversalSource g) => g.V().hasId('1'),
  ],
  'g_addV_propertyXidvar_1varX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV().property(t.id, xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().hasId('1'),
  ],
  'g_addV_propertyXmapX': <Function>[
    (GraphTraversalSource g) => g.addV().property({'name': 'foo', 'age': GInt(42)}),
    (GraphTraversalSource g) => g.V().has('name', 'foo'),
  ],
  'g_addV_propertyXsingle_mapX': <Function>[
    (GraphTraversalSource g) => g.addV().property(cardinality.single, {'name': 'foo', 'age': GInt(42)}),
    (GraphTraversalSource g) => g.V().has('name', 'foo'),
  ],
  'g_V_hasXname_fooX_propertyXname_setXbarX_age_43X': <Function>[
    (GraphTraversalSource g) => g.addV().property(cardinality.single, 'name', 'foo').property('age', GInt(42)),
    (GraphTraversalSource g) => g.V().has('name', 'foo').property({'name': cardinality.set_('bar'), 'age': GInt(43)}),
    (GraphTraversalSource g) => g.V().has('name', 'foo'),
    (GraphTraversalSource g) => g.V().has('name', 'bar'),
    (GraphTraversalSource g) => g.V().has('age', GInt(43)),
    (GraphTraversalSource g) => g.V().has('age', GInt(42)),
  ],
  'g_V_hasXname_fooX_propertyXset_name_bar_age_singleX43XX': <Function>[
    (GraphTraversalSource g) => g.addV().property(cardinality.single, 'name', 'foo').property('age', GInt(42)),
    (GraphTraversalSource g) => g.V().has('name', 'foo').property(cardinality.set_, {'name': 'bar', 'age': cardinality.single(GInt(43))}),
    (GraphTraversalSource g) => g.V().has('name', 'foo'),
    (GraphTraversalSource g) => g.V().has('name', 'bar'),
    (GraphTraversalSource g) => g.V().has('age', GInt(43)),
    (GraphTraversalSource g) => g.V().has('age', GInt(42)),
  ],
  'g_addV_propertyXnullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property(null),
    (GraphTraversalSource g) => g.V().hasLabel('person').values(),
  ],
  'g_addV_propertyXemptyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property({}),
    (GraphTraversalSource g) => g.V().hasLabel('person').values(),
  ],
  'g_addV_propertyXset_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('foo').property(cardinality.set_, null),
    (GraphTraversalSource g) => g.V().hasLabel('foo').values(),
  ],
  'g_addV_propertyXset_emptyX': <Function>[
    (GraphTraversalSource g) => g.addV('foo').property(cardinality.set_, {}),
    (GraphTraversalSource g) => g.V().hasLabel('person').values(),
  ],
  'g_addVXpersonX_propertyXname_joshX_propertyXage_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'josh').property('age', null),
    (GraphTraversalSource g) => g.V().has('person', 'age', null),
  ],
  'g_addVXpersonX_propertyXname_markoX_propertyXfriendWeight_null_acl_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('friendWeight', null, 'acl', null),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('friendWeight', null),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('friendWeight').has('acl', null),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('friendWeight').count(),
  ],
  'g_V_hasXperson_name_aliceX_propertyXsingle_age_unionXage_constantX1XX_sumX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').property(cardinality.single, 'age', GInt(50)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').property('age', Anon.union(Anon.values('age'), Anon.constant(GInt(1))).sum()),
    (GraphTraversalSource g) => g.V().has('person', 'age', GInt(50)),
    (GraphTraversalSource g) => g.V().has('person', 'age', GInt(51)),
  ],
  'g_V_limitX3X_addVXsoftwareX_aggregateXa1X_byXlabelX_aggregateXa2X_byXlabelX_capXa1_a2X_selectXa_bX_byXunfoldX_foldX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().limit(GInt(3)).addV('software').aggregate('a1').by(t.label).aggregate('a2').by(t.label).cap('a1', 'a2').select('a1', 'a2').by(Anon.unfold().fold()),
  ],
  'g_addV_propertyXname_markoX_withXkey_valueX_valuesXname_keyX': <Function>[
    (GraphTraversalSource g) => g.addV().property('name', 'marko').with_('key', 'value').values('name', 'key'),
  ],
  'g_addV_propertyXname_marko_since_2010X_withXkey_valueX_propertiesXnameX_valuesXsince_keyX': <Function>[
    (GraphTraversalSource g) => g.addV().property('name', 'marko', 'since', GInt(2010)).with_('key', 'value').properties('name').values('since', 'key'),
  ],
  'g_injectX1X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).asBool(),
  ],
  'g_injectX3_14X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(3.14)).asBool(),
  ],
  'g_injectXneg_1X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(-1)).asBool(),
  ],
  'g_injectX0X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(0)).asBool(),
  ],
  'g_injectXneg_0X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(-0.0)).asBool(),
  ],
  'g_injectXNaNX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).asBool(),
  ],
  'g_injectXbool_trueX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(true).asBool(),
  ],
  'g_injectXfalseX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(false).asBool(),
  ],
  'g_injectXtrueX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject('true').asBool(),
  ],
  'g_injectXmixed_trueX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject('tRUe').asBool(),
  ],
  'g_injectXnullX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject(null).asBool(),
  ],
  'g_injectXhelloX_asBool': <Function>[
    (GraphTraversalSource g) => g.inject('hello').asBool(),
  ],
  'g_injectX1_2X_asBool': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).asBool(),
  ],
  'g_VXX_localX_outE_countX_asBool': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.outE().count()).asBool(),
  ],
  'g_V_sackXassignX_byX_hasLabelXpersonX_count_asBoolX_sackXandX_byX_outE_count_asBoolX_sack_path': <Function>[
    (GraphTraversalSource g) => g.V().sack(operator_.assign).by(Anon.hasLabel('person').count().asBool()).sack(operator_.and_).by(Anon.outE().count().asBool()).sack().path(),
  ],
  'g_injectXstrX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject('2023-08-02T00:00:00Z').asDate(),
  ],
  'g_injectXstr_offsetX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject('2023-08-02T00:00:00-07:00').asDate(),
  ],
  'g_injectX1694017707000X_asDate': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1694017707000)).asDate(),
  ],
  'g_injectX1694017708000LX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject(GLong(1694017708000)).asDate(),
  ],
  'g_injectX1694017709000dX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1694017709000.1)).asDate(),
  ],
  'g_injectX1_2X_asDate': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).asDate(),
  ],
  'g_injectXnullX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject(null).asDate(),
  ],
  'g_injectXinvalidstrX_asDate': <Function>[
    (GraphTraversalSource g) => g.inject('This String is not an ISO 8601 Date').asDate(),
  ],
  'g_V_valuesXbirthdayX_asDate_asNumber_asDate': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').property('birthday', '2020-08-02').addV('person').property('name', 'john').property('birthday', '1988-12-10').addV('person').property('name', 'charlie').property('birthday', '2002-02-01').addV('person').property('name', 'suzy').property('birthday', '1965-10-31'),
    (GraphTraversalSource g) => g.V().values('birthday').asDate().asNumber().asDate(),
  ],
  'g_injectX5bX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(5)).asNumber(),
  ],
  'g_injectX5sX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GShort(5)).asNumber(),
  ],
  'g_injectX5iX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(5)).asNumber(),
  ],
  'g_injectX5lX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GLong(5)).asNumber(),
  ],
  'g_injectX5nX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(BigInt.parse('5')).asNumber(),
  ],
  'g_injectX5_0X_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(5.0)).asNumber(),
  ],
  'g_injectX5_75fX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject(GFloat(5.75)).asNumber(),
  ],
  'g_injectX5X_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject('5').asNumber(),
  ],
  'g_injectXtestX_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject('test').asNumber(),
  ],
  'g_injectX_1_2_3_4X_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3), GInt(4)]).asNumber(),
  ],
  'g_injectX1_2_3_4X_unfold_asNumber': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2), GInt(3), GInt(4)]).unfold().asNumber(),
  ],
  'g_injectX_1__2__3__4_X_asNumberXX_foldXX': <Function>[
    (GraphTraversalSource g) => g.inject('1', GInt(2), '3', GInt(4)).asNumber().fold(),
  ],
  'g_injectX5_43X_asNumberXGType_INTX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(5.43)).asNumber(gtype.INT),
  ],
  'g_injectX5_67X_asNumberXGType_INTX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(5.67)).asNumber(gtype.INT),
  ],
  'g_injectX5X_asNumberXGType_LONGX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(5)).asNumber(gtype.LONG),
  ],
  'g_injectX12X_asNumberXGType_BYTEX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(12)).asNumber(gtype.BYTE),
  ],
  'g_injectX32768X_asNumberXGType_SHORTX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(32768)).asNumber(gtype.SHORT),
  ],
  'g_injectX300X_asNumberXGType_BYTEX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(300)).asNumber(gtype.BYTE),
  ],
  'g_injectX32768X_asNumberXGType_VertexX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(32768)).asNumber(gtype.VERTEX),
  ],
  'g_injectX5X_asNumberXGType_BYTEX': <Function>[
    (GraphTraversalSource g) => g.inject('5').asNumber(gtype.BYTE),
  ],
  'g_injectX1_000X_asNumberXGType_BIGINTX': <Function>[
    (GraphTraversalSource g) => g.inject('1,000').asNumber(gtype.BIGINT),
  ],
  'g_injectX1_2_3_4_0x5X_asNumber_sum_asNumberXGType_BYTEX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0), GInt(2), GInt(3), '4', '0x5').asNumber().sum().asNumber(gtype.BYTE),
  ],
  'g_injectXnullX_asNumberXGType_INTX': <Function>[
    (GraphTraversalSource g) => g.inject(null).asNumber(gtype.INT),
  ],
  'g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX_asNumberXGType_INTX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('knows').as_('b').math_('a + b').by('age').asNumber(gtype.INT),
  ],
  'g_withSideEffectXx_100X_V_age_mathX__plus_xX_asNumberXGType_LONGX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('x', GInt(100)).V().values('age').math_('_ + x').asNumber(gtype.LONG),
  ],
  'g_V_valuesXageX_asString_asNumberXGType_DOUBLEX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').asString().asNumber(gtype.DOUBLE),
  ],
  'g_V_valuesXbirthdayX_asNumber_asDate_asNumber': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').property('birthday', GInt(1596326400000)).addV('person').property('name', 'john').property('birthday', GInt(597715200000)).addV('person').property('name', 'charlie').property('birthday', GInt(1012521600000)).addV('person').property('name', 'suzy').property('birthday', GInt(-131587200000)),
    (GraphTraversalSource g) => g.V().values('birthday').asNumber().asDate().asNumber(),
  ],
  'g_injectX1_2X_asString': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1), GInt(2)).asString(),
  ],
  'g_injectX1_2X_asStringXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1), GInt(2)).asString(scope.local),
  ],
  'g_injectXlist_1_2X_asStringXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).asString(scope.local),
  ],
  'g_injectX1_nullX_asString': <Function>[
    (GraphTraversalSource g) => g.inject(null, GInt(1)).asString(),
  ],
  'g_injectX1_nullX_asStringXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), null]).asString(scope.local),
  ],
  'g_V_valueMapXnameX_asString': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name').asString(),
  ],
  'g_V_valueMapXnameX_order_fold_asStringXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name').order().fold().asString(scope.local),
  ],
  'g_V_asString': <Function>[
    (GraphTraversalSource g) => g.V().asString(),
  ],
  'g_V_fold_asStringXlocalX_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().fold().asString(scope.local).order(scope.local),
  ],
  'g_E_asString': <Function>[
    (GraphTraversalSource g) => g.E().asString(),
  ],
  'g_V_properties': <Function>[
    (GraphTraversalSource g) => g.V().properties().asString(),
  ],
  'g_V_hasLabelXpersonX_valuesXageX_asString': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('age').asString(),
  ],
  'g_V_hasLabelXpersonX_valuesXageX_order_fold_asStringXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('age').order().fold().asString(scope.local),
  ],
  'g_V_hasLabelXpersonX_valuesXageX_asString_concatX_years_oldX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('age').asString().concat(' years old'),
  ],
  'g_call': <Function>[
    (GraphTraversalSource g) => g.call(),
  ],
  'g_callXlistX': <Function>[
    (GraphTraversalSource g) => g.call('--list'),
  ],
  'g_callXlistX_withXstring_stringX': <Function>[
    (GraphTraversalSource g) => g.call('--list').with_('service', 'tinker.search'),
  ],
  'g_callXlistX_withXstring_traversalX': <Function>[
    (GraphTraversalSource g) => g.call('--list').with_('service', Anon.constant('tinker.search')),
  ],
  'g_callXlist_mapX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('--list', xx1),
  ],
  'g_callXlist_traversalX': <Function>[
    (GraphTraversalSource g) => g.call('--list', Anon.project('service').by(Anon.constant('tinker.search'))),
  ],
  'g_callXlist_map_traversalX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('--list', xx1, Anon.project('service').by(Anon.constant('tinker.search'))),
  ],
  'g_callXsearch_mapX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('tinker.search', xx1).element(),
  ],
  'g_callXsearch_traversalX': <Function>[
    (GraphTraversalSource g) => g.call('tinker.search', Anon.project('search').by(Anon.constant('vada'))).element(),
  ],
  'g_callXsearchX_withXstring_stringX': <Function>[
    (GraphTraversalSource g) => g.call('tinker.search').with_('search', 'vada').element(),
  ],
  'g_callXsearchX_withXstring_traversalX': <Function>[
    (GraphTraversalSource g) => g.call('tinker.search').with_('search', Anon.constant('vada')).element(),
  ],
  'g_callXsearch_mapX_withXstring_VertexX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('tinker.search', xx1).with_('type', 'Vertex').element(),
  ],
  'g_callXsearch_mapX_withXstring_EdgeX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('tinker.search', xx1).with_('type', 'Edge').element(),
  ],
  'g_callXsearch_mapX_withXstring_VertexPropertyX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.call('tinker.search', xx1).with_('type', 'VertexProperty').element(),
  ],
  'g_V_callXdcX': <Function>[
    (GraphTraversalSource g) => g.V().as_('v').call('tinker.degree.centrality').project('vertex', 'degree').by(Anon.select('v')).by(),
  ],
  'g_V_whereXcallXdcXX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.call('tinker.degree.centrality').is_(GInt(3))),
  ],
  'g_V_callXdcX_withXdirection_OUTX': <Function>[
    (GraphTraversalSource g) => g.V().as_('v').call('tinker.degree.centrality').with_('direction', direction.OUT).project('vertex', 'degree').by(Anon.select('v')).by(),
  ],
  'g_V_callXdc_mapX_withXdirection_OUTX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('v').call('tinker.degree.centrality', xx1).with_('direction', direction.OUT).project('vertex', 'degree').by(Anon.select('v')).by(),
  ],
  'g_V_callXdc_traversalX': <Function>[
    (GraphTraversalSource g) => g.V().as_('v').call('tinker.degree.centrality', Anon.project('direction').by(Anon.constant(direction.OUT))).project('vertex', 'degree').by(Anon.select('v')).by(),
  ],
  'g_V_callXdc_map_traversalX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('v').call('tinker.degree.centrality', xx1, Anon.project('direction').by(Anon.constant(direction.OUT))).project('vertex', 'degree').by(Anon.select('v')).by(),
  ],
  'g_V_coalesceXoutXfooX_outXbarXX': <Function>[
    (GraphTraversalSource g) => g.V().coalesce(Anon.out('foo'), Anon.out('bar')),
  ],
  'g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).coalesce(Anon.out('knows'), Anon.out('created')).values('name'),
  ],
  'g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).coalesce(Anon.out('created'), Anon.out('knows')).values('name'),
  ],
  'g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().coalesce(Anon.out('likes'), Anon.out('knows'), Anon.out('created')).groupCount().by('name'),
  ],
  'g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX': <Function>[
    (GraphTraversalSource g) => g.V().coalesce(Anon.outE('knows'), Anon.outE('created')).otherV().path().by('name').by(t.label),
  ],
  'g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').order().by('name').coalesce(Anon.values('name'), Anon.constant('x')),
  ],
  'g_injectXnullX_combineXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).combine(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_combineXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').combine(Anon.V().fold()),
  ],
  'g_V_fold_combineXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().combine(Anon.constant(null)),
  ],
  'g_V_fold_combineXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().combine(Anon.V()),
  ],
  'g_V_valuesXnameX_fold_combineX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().combine(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_combineXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().combine(null),
  ],
  'g_V_valuesXnonexistantX_fold_combineXV_valuesXnameX_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().combine(Anon.V().values('name').fold()).unfold(),
  ],
  'g_V_valuesXnameX_fold_combineXV_valuesXnonexistantX_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().combine(Anon.V().values('nonexistant').fold()).unfold(),
  ],
  'g_V_valuesXageX_order_byXdescX_fold_combineXV_valuesXageX_order_byXdescX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).fold().combine(Anon.V().values('age').order().by(order.desc).fold()),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_combineXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).combine(['MARKO']),
  ],
  'g_injectXxx1X_combineXV_valuesXnameX_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).combine(Anon.V().values('name').fold()).unfold(),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_combineXseattle_vancouverX_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().combine(['seattle', 'vancouver']).order(scope.local),
  ],
  'g_V_out_out_path_byXnameX_combineXempty_listX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').combine([]),
  ],
  'g_V_valuesXageX_order_fold_combineXconstantX27X_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().fold().combine(Anon.constant(GInt(27)).fold()),
  ],
  'g_V_out_out_path_byXnameX_combineXdave_kelvinX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').combine(['dave', 'kelvin']),
  ],
  'g_injectXa_null_bX_combineXa_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).combine(['a', 'c']),
  ],
  'g_injectXa_null_bX_combineXa_null_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).combine(['a', null, 'c']),
  ],
  'g_injectX3_threeX_combineXfive_three_7X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).combine(['five', 'three', GInt(7)]),
  ],
  'g_injectXa_bX_concat': <Function>[
    (GraphTraversalSource g) => g.inject('a', 'b').concat(),
  ],
  'g_injectXa_bX_concat_XcX': <Function>[
    (GraphTraversalSource g) => g.inject('a', 'b').concat('c'),
  ],
  'g_injectXa_bX_concat_Xc_dX': <Function>[
    (GraphTraversalSource g) => g.inject('a', 'b').concat('c', 'd'),
  ],
  'g_injectXa_bX_concat_Xinject_c_dX': <Function>[
    (GraphTraversalSource g) => g.inject('a', 'b').concat(Anon.inject('c')),
  ],
  'g_injectXaX_concat_Xinject_List_b_cX': <Function>[
    (GraphTraversalSource g) => g.inject('a').concat(Anon.inject(['b', 'c'])),
  ],
  'g_injectXListXa_bXcX_concat_XdX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b'], 'c').concat('d'),
  ],
  'g_injectXnullX_concat_XinjectX': <Function>[
    (GraphTraversalSource g) => g.inject(null).concat(),
  ],
  'g_injectXnull_aX_concat_Xnull_bX': <Function>[
    (GraphTraversalSource g) => g.inject(null, 'a').concat(null, 'b'),
  ],
  'g_injectXhello_hiX_concatXV_values_order_byXnameX_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.inject('hello', 'hi').concat(Anon.V().order().by('name').values('name')),
  ],
  'g_V_hasLabel_value_concat_X_X_concat_XpersonX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').concat(' ').concat('person'),
  ],
  'g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselectXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').constant('Mr.').concat(Anon.select('a')),
  ],
  'g_hasLabelXsoftwareX_asXaX_valuesXnameX_concatXunsesX_concatXselectXaXvaluesXlangX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').as_('a').values('name').concat(' uses ').concat(Anon.select('a').values('lang')),
  ],
  'g_VX1X_outE_asXaX_VX1X_valuesXnamesX_concatXselectXaX_labelX_concatXselectXaX_inV_valuesXnameXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().as_('a').V(vid1).values('name').concat(Anon.select('a').label()).concat(Anon.select('a').inV().values('name')),
  ],
  'g_VX1X_outE_asXaX_VX1X_valuesXnamesX_concatXselectXaX_label_selectXaX_inV_valuesXnameXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().as_('a').V(vid1).values('name').concat(Anon.select('a').label(), Anon.select('a').inV().values('name')),
  ],
  'g_addVXconstantXprefix_X_concatXVX1X_labelX_label': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid1}) => g.addV(Anon.constant('prefix_').concat(Anon.V(vid1).label())).label(),
  ],
  'g_injectXnullX_conjoinX1X': <Function>[
    (GraphTraversalSource g) => g.inject(null).conjoin('1'),
  ],
  'g_V_valuesXnameX_conjoinX1X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').conjoin('1'),
  ],
  'g_V_valuesXnonexistantX_fold_conjoinX_X': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().conjoin(';'),
  ],
  'g_V_valuesXnameX_order_fold_conjoinX_X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().fold().conjoin('_'),
  ],
  'g_V_valuesXageX_order_fold_conjoinXsemicolonX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().fold().conjoin(';'),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_conjoinXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).conjoin('MARKO'),
  ],
  'g_injectXmarkoX_conjoinX_X': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).conjoin('-'),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_orderXlocalX_conjoinX1X': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().order(scope.local).conjoin('1'),
  ],
  'g_V_out_out_path_byXnameX_conjoinXX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').conjoin(''),
  ],
  'g_injectXa_null_bX_conjoinXxyzX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).conjoin('xyz'),
  ],
  'g_injectX3_threeX_conjoinX_X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).conjoin(';'),
  ],
  'g_injectXnull_a_null_bX_conjoinXplusX': <Function>[
    (GraphTraversalSource g) => g.inject([null, 'a', null, 'b']).conjoin('+'),
  ],
  'g_injectXnull_nullX_conjoinXplusX': <Function>[
    (GraphTraversalSource g) => g.inject([null, null]).conjoin('+'),
  ],
  'g_V_connectedComponent_hasXcomponentX': <Function>[
    (GraphTraversalSource g) => g.V().connectedComponent().has('gremlin.connectedComponentVertexProgram.component'),
  ],
  'g_V_dedup_connectedComponent_hasXcomponentX': <Function>[
    (GraphTraversalSource g) => g.V().dedup().connectedComponent().has('gremlin.connectedComponentVertexProgram.component'),
  ],
  'g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').connectedComponent().project('name', 'component').by('name').by('gremlin.connectedComponentVertexProgram.component'),
  ],
  'g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').connectedComponent().with_('~tinkerpop.connectedComponent.edges', Anon.bothE('knows')).with_('~tinkerpop.connectedComponent.propertyName', 'cluster').project('name', 'cluster').by('name').by('cluster'),
  ],
  'g_V_constantX123X': <Function>[
    (GraphTraversalSource g) => g.V().constant(GInt(123)),
  ],
  'g_V_constantXnullX': <Function>[
    (GraphTraversalSource g) => g.V().constant(null),
  ],
  'g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.hasLabel('person'), Anon.values('name'), Anon.constant('inhuman')),
  ],
  'g_V_count': <Function>[
    (GraphTraversalSource g) => g.V().count(),
  ],
  'g_V_out_count': <Function>[
    (GraphTraversalSource g) => g.V().out().count(),
  ],
  'g_V_both_both_count': <Function>[
    (GraphTraversalSource g) => g.V().both().both().count(),
  ],
  'g_V_fold_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().fold().count(scope.local),
  ],
  'g_V_hasXnoX_count': <Function>[
    (GraphTraversalSource g) => g.V().has('no').count(),
  ],
  'g_V_whereXinXkknowsX_outXcreatedX_count_is_0XX_name': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.in_('knows').out('created').count().is_(GInt(0))).values('name'),
  ],
  'g_V_repeatXoutX_timesX8X_count': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(8)).count(),
  ],
  'g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(5)).as_('a').out('writtenBy').as_('b').select('a', 'b').count(),
  ],
  'g_V_repeatXoutX_timesX3X_count': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(3)).count(),
  ],
  'g_V_order_byXlangX_count': <Function>[
    (GraphTraversalSource g) => g.V().order().by('lang').count(),
  ],
  'g_E_sampleX1X_count': <Function>[
    (GraphTraversalSource g) => g.E().sample(GInt(1)).count(),
  ],
  'g_V_sampleX1X_byXageX_count': <Function>[
    (GraphTraversalSource g) => g.V().sample(GInt(1)).by('age').count(),
  ],
  'g_V_order_byXnoX_count': <Function>[
    (GraphTraversalSource g) => g.V().order().by('no').count(),
  ],
  'g_V_group_byXlabelX_count': <Function>[
    (GraphTraversalSource g) => g.V().group().by(t.label).count(),
  ],
  'g_V_group_byXlabelX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(t.label).count(scope.local),
  ],
  'g_injectXdatetimeXstrXX_dateAddXDT_hour_2X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateAdd(dt.hour, GInt(2)),
  ],
  'g_injectXdatetimeXstrXX_dateAddXhour_2X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateAdd(dt.hour, GInt(2)),
  ],
  'g_injectXdatetimeXstrXX_dateAddXhour_1X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateAdd(dt.hour, GInt(-1)),
  ],
  'g_injectXdatetimeXstrXX_dateAddXminute_10X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateAdd(dt.minute, GInt(10)),
  ],
  'g_injectXdatetimeXstrXX_dateAddXsecond_20X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateAdd(dt.second, GInt(20)),
  ],
  'g_injectXdatetimeXstrXX_dateAddXday_11X': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-09-06T00:00Z'), DateTime.parse('2023-09-06T00:00Z')).dateAdd(dt.day, GInt(11)),
  ],
  'g_injectXdatetimeXstr1XX_dateDiffXdatetimeXstr2XX': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-02T00:00Z'), DateTime.parse('2023-08-02T00:00Z')).dateDiff(DateTime.parse('2023-08-09T00:00Z')),
  ],
  'g_injectXdatetimeXstr1XX_dateDiffXconstantXdatetimeXstr2XXX': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-08T00:00Z'), DateTime.parse('2023-08-08T00:00Z')).dateDiff(Anon.constant(DateTime.parse('2023-08-01T00:00Z'))),
  ],
  'g_injectXdatetimeXstr1XX_dateDiffXinjectXdatetimeXstr2XXX': <Function>[
    (GraphTraversalSource g) => g.inject(DateTime.parse('2023-08-08T00:00Z'), DateTime.parse('2023-08-08T00:00Z')).dateDiff(Anon.inject(DateTime.parse('2023-10-11T00:00Z'))),
  ],
  'g_V_valuesXbirthdayX_asDate_dateDiffXdatetimeX19700101T0000ZXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').property('birthday', '1596326400000').addV('person').property('name', 'john').property('birthday', '597715200000').addV('person').property('name', 'charlie').property('birthday', '1012521600000').addV('person').property('name', 'suzy').property('birthday', '-131587200000'),
    (GraphTraversalSource g) => g.V().values('birthday').asNumber().asDate().dateDiff(DateTime.parse('1970-01-01T00:00Z')),
  ],
  'g_V_hasXname_aliceX_valuesXbirthdayX_asDate_dateDiffXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').property('birthday', GInt(1596326400000)),
    (GraphTraversalSource g) => g.V().has('name', 'alice').values('birthday').asDate().dateDiff(Anon.constant(null)),
  ],
  'g_injectXnullX_differenceXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).difference(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_differenceXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').difference(Anon.V().fold()),
  ],
  'g_V_fold_differenceXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().difference(Anon.constant(null)),
  ],
  'g_V_fold_differenceXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().difference(Anon.V()),
  ],
  'g_V_valuesXnameX_fold_differenceX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().difference(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_differenceXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().difference(null),
  ],
  'g_V_valuesXnonexistantX_fold_differenceXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().difference(Anon.V().values('name').fold()),
  ],
  'g_V_valuesXnameX_fold_differenceXV_valuesXnonexistantX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().difference(Anon.V().values('nonexistant').fold()),
  ],
  'g_V_valuesXageX_fold_differenceXV_valuesXageX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().difference(Anon.V().values('age').fold()),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_differenceXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).difference(['MARKO']),
  ],
  'g_injectXmarkoX_differenceXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).difference(Anon.V().values('name').fold()),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_differenceXseattle_vancouverX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().difference(['seattle', 'vancouver']),
  ],
  'g_V_out_out_path_byXnameX_differenceXrippleX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').difference(['ripple']),
  ],
  'g_V_out_out_path_byXnameX_differenceXempty_listX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').difference([]),
  ],
  'g_V_valuesXageX_fold_differenceXconstantX27X_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().difference(Anon.constant(GInt(27)).fold()),
  ],
  'g_V_out_out_path_byXnameX_differenceXdave_kelvinX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').difference(['dave', 'kelvin']),
  ],
  'g_injectXa_null_bX_differenceXa_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).difference(['a', 'c']),
  ],
  'g_injectXa_null_bX_differenceXa_null_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).difference(['a', null, 'c']),
  ],
  'g_injectX3_threeX_differenceXfive_three_7X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).difference(['five', 'three', GInt(7)]),
  ],
  'g_injectXnullX_disjunctXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).disjunct(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_disjunctXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').disjunct(Anon.V().fold()),
  ],
  'g_V_fold_disjunctXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().disjunct(Anon.constant(null)),
  ],
  'g_V_fold_disjunctXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().disjunct(Anon.V()),
  ],
  'g_V_valuesXnameX_fold_disjunctX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().disjunct(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_disjunctXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().disjunct(null),
  ],
  'g_V_valuesXnonexistantX_fold_disjunctXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().disjunct(Anon.V().values('name').fold()),
  ],
  'g_V_valuesXnameX_fold_disjunctXV_valuesXnonexistantX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().disjunct(Anon.V().values('nonexistant').fold()),
  ],
  'g_V_valuesXageX_fold_disjunctXV_valuesXageX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().disjunct(Anon.V().values('age').fold()),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_disjunctXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).disjunct(['MARKO']),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_disjunctXseattle_vancouverX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().disjunct(['seattle', 'vancouver']),
  ],
  'g_V_out_out_path_byXnameX_disjunctXmarkoX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').disjunct(['marko']),
  ],
  'g_V_out_out_path_byXnameX_disjunctXstephen_markoX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').disjunct(['stephen', 'marko']),
  ],
  'g_V_out_out_path_byXnameX_disjunctXdave_kelvinX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').disjunct(['dave', 'kelvin']),
  ],
  'g_injectXa_null_bX_disjunctXa_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).disjunct(['a', 'c']),
  ],
  'g_injectXa_null_bX_disjunctXa_null_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).disjunct(['a', null, 'c']),
  ],
  'g_injectX3_threeX_disjunctXfive_three_7X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).disjunct(['five', 'three', GInt(7)]),
  ],
  'g_E': <Function>[
    (GraphTraversalSource g) => g.E(),
  ],
  'g_EX11X': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11),
  ],
  'g_EX11AsStringX': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11),
  ],
  'g_EXeid7_eid11X': <Function>[
    (GraphTraversalSource g, {dynamic eid11, dynamic eid7}) => g.E(eid7, eid11),
  ],
  'g_EXlistXeid7_eid11XX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.E(xx1),
  ],
  'g_EXnullX': <Function>[
    (GraphTraversalSource g) => g.E(null),
  ],
  'g_EXlistXnullXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.E(xx1),
  ],
  'g_EX11_nullX': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11, null),
  ],
  'g_V_EX11X': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.V().E(eid11),
  ],
  'g_EX11X_E': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11).E(),
  ],
  'g_V_EXnullX': <Function>[
    (GraphTraversalSource g) => g.V().E(null),
  ],
  'g_V_EXlistXnullXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().E(xx1),
  ],
  'g_injectX1X_EX11_nullX': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.inject(GInt(1)).E(eid11, null),
  ],
  'g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'josh').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g) => g.inject(GInt(1)).coalesce(Anon.E().hasLabel('tests'), Anon.addE('tests').from_(Anon.V().has('name', 'josh')).to(Anon.V().has('name', 'vadas'))),
    (GraphTraversalSource g) => g.E().hasLabel('tests'),
  ],
  'g_VX1X_outE_inV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().inV(),
  ],
  'g_VX2X_inE_outV': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).inE().outV(),
  ],
  'g_V_outE_hasXweight_1X_outV': <Function>[
    (GraphTraversalSource g) => g.V().outE().has('weight', GDouble(1.0)).outV(),
  ],
  'g_VX1X_outE_otherV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().otherV(),
  ],
  'g_VX4X_bothE_otherV': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).bothE().otherV(),
  ],
  'g_VX4X_bothE_hasXweight_lt_1X_otherV': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).bothE().has('weight', P.lt(GDouble(1.0))).otherV(),
  ],
  'get_g_VX1X_outE_otherV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().otherV(),
  ],
  'g_VX1X_outEXknowsX_inV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').inV(),
  ],
  'g_VX1X_outEXknows_createdX_inV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows', 'created').inV(),
  ],
  'g_VX1X_outEXknowsX_bothV': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').bothV(),
  ],
  'g_VX1X_outEXknowsX_bothV_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').bothV().values('name'),
  ],
  'g_V_toEXout_knowsvarX_valuesXweightX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().toE(direction.OUT, xx1).values('weight'),
  ],
  'g_VX1X_properties_element': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).properties().element().limit(GInt(1)),
  ],
  'g_V_properties_element': <Function>[
    (GraphTraversalSource g) => g.V().properties().element(),
  ],
  'g_V_propertiesXageX_element': <Function>[
    (GraphTraversalSource g) => g.V().properties('age').element(),
  ],
  'g_EX_properties_element': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11).properties().element().limit(GInt(1)),
  ],
  'g_E_properties_element': <Function>[
    (GraphTraversalSource g) => g.E().properties().element(),
  ],
  'g_VXv7_properties_properties_element_element': <Function>[
    (GraphTraversalSource g, {dynamic vid7}) => g.V(vid7).properties().properties().element().element().limit(GInt(1)),
  ],
  'g_V_properties_properties_element_element': <Function>[
    (GraphTraversalSource g, {dynamic vid7}) => g.V(vid7).properties().properties().element().element(),
  ],
  'g_V_elementMap': <Function>[
    (GraphTraversalSource g) => g.V().elementMap(),
  ],
  'g_V_elementMapXname_ageX': <Function>[
    (GraphTraversalSource g) => g.V().elementMap('name', 'age'),
  ],
  'g_EX11X_elementMap': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11).elementMap(),
  ],
  'g_V_elementMapXname_age_nullX': <Function>[
    (GraphTraversalSource g) => g.V().elementMap('name', 'age', null),
  ],
  'g_V_asXaX_flatMapXselectXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').flatMap(Anon.select('a')),
  ],
  'g_V_valuesXnameX_flatMapXsplitXaX_unfoldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').flatMap(Anon.split('a').unfold()),
  ],
  'g_V_flatMapXout_outX_path': <Function>[
    (GraphTraversalSource g) => g.V().flatMap(Anon.out().out()).path(),
  ],
  'g_V_fold': <Function>[
    (GraphTraversalSource g) => g.V().fold(),
  ],
  'g_V_fold_unfold': <Function>[
    (GraphTraversalSource g) => g.V().fold().unfold(),
  ],
  'g_V_age_foldX0_plusX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold(GInt(0), operator_.sum),
  ],
  'g_injectXa1_b2X_foldXm_addAllX': <Function>[
    (GraphTraversalSource g) => g.inject({'a': GInt(1)}, {'b': GInt(2)}).fold({}, operator_.addAll),
  ],
  'g_injectXa1_b2_b4X_foldXm_addAllX': <Function>[
    (GraphTraversalSource g) => g.inject({'a': GInt(1)}, {'b': GInt(2)}, {'b': GInt(4)}).fold({}, operator_.addAll),
  ],
  'g_injectXlist1_list2X_fold': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)], [GInt(3), GInt(4)]).fold(),
  ],
  'g_injectXlist1_list2_list3X_fold': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)], [GInt(3), GInt(4)], [GInt(5), GInt(6)]).fold(),
  ],
  'g_VX1X_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').format_('Hello world'),
  ],
  'g_V_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().format_('%{name} is %{age} years old'),
  ],
  'g_injectX1X_asXageX_V_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).as_('age').V().format_('%{name} is %{age} years old'),
  ],
  'g_V_formatXstrX_byXvaluesXnameXX_byXvaluesXageXX': <Function>[
    (GraphTraversalSource g) => g.V().format_('%{_} is %{_} years old').by(Anon.values('name')).by(Anon.values('age')),
  ],
  'g_V_hasLabelXpersonX_formatXstrX_byXconstantXhelloXX_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').format_('%{_} %{_} %{_}').by(Anon.constant('hello')).by(Anon.values('name')),
  ],
  'g_VX1X_formatXstrX_byXconstantXhelloXX_byXvaluesXnameXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).format_('%{_}').by(Anon.constant('hello')).by(Anon.values('name')),
  ],
  'g_V_formatXstrX_byXbothE_countX': <Function>[
    (GraphTraversalSource g) => g.V().format_('%{name} has %{_} connections').by(Anon.bothE().count()),
  ],
  'g_V_projectXname_countX_byXvaluesXnameXX_byXbothE_countX_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().project('name', 'count').by(Anon.values('name')).by(Anon.bothE().count()).format_('%{name} has %{count} connections'),
  ],
  'g_V_elementMap_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().elementMap().format_('%{name} is %{age} years old'),
  ],
  'g_V_hasLabelXpersonX_asXaX_valuesXnameX_asXp1X_selectXaX_inXknowsX_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('a').values('name').as_('p1').select('a').in_('knows').format_('%{p1} knows %{name}'),
  ],
  'g_V_asXsX_label_asXsubjectX_selectXsX_outE_asXpX_label_asXpredicateX_selectXpX_inV_label_asXobjectX_formatXstrX': <Function>[
    (GraphTraversalSource g) => g.V().as_('s').label().as_('subject').select('s').outE().as_('p').label().as_('predicate').select('p').inV().label().as_('object').format_('%{subject} %{predicate} %{object}'),
  ],
  'g_V_hasLabelXsoftwareX_index_unfold': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').index().unfold(),
  ],
  'g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').order().by('name').index().with_(WithOptions.indexer, WithOptions.map_),
  ],
  'g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').fold().order(scope.local).index().unfold().order().by(Anon.tail(scope.local, GInt(1))),
  ],
  'g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').fold().order(scope.local).index().with_(WithOptions.indexer, WithOptions.map_),
  ],
  'g_VX1X_valuesXageX_index_unfold_unfold': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').index().unfold().unfold(),
  ],
  'g_injectXnullX_intersectXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).intersect(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_intersectXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').intersect(Anon.V().fold()),
  ],
  'g_V_fold_intersectXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().intersect(Anon.constant(null)),
  ],
  'g_V_fold_intersectXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().intersect(Anon.V()),
  ],
  'g_V_valuesXnameX_fold_intersectX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().intersect(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_intersectXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().intersect(null),
  ],
  'g_V_valuesXnonexistantX_fold_intersectXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().intersect(Anon.V().values('name').fold()),
  ],
  'g_V_valuesXnameX_fold_intersectXV_valuesXnonexistantX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().intersect(Anon.V().values('nonexistant').fold()),
  ],
  'g_V_valuesXageX_fold_intersectXV_valuesXageX_foldX_order_local': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().intersect(Anon.V().values('age').fold()).order(scope.local),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_intersectXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).intersect(['MARKO']),
  ],
  'g_injectXmarkoX_intersectX___V_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).intersect(Anon.V().values('name').fold()),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_intersectXseattle_vancouverX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().intersect(['seattle', 'vancouver']),
  ],
  'g_V_valuesXageX_fold_intersectX___constantX27X_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().intersect(Anon.constant(GInt(27)).fold()),
  ],
  'g_V_out_out_path_byXnameX_intersectXdave_kelvinX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').intersect(['dave', 'kelvin']),
  ],
  'g_injectXa_null_bX_intersectXa_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).intersect(['a', 'c']),
  ],
  'g_injectXa_null_bX_intersectXa_null_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).intersect(['a', null, 'c']),
  ],
  'g_injectX3_threeX_intersectXfive_three_7X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).intersect(['five', 'three', GInt(7)]),
  ],
  'g_injectX__feature___test__nullX_lTrim': <Function>[
    (GraphTraversalSource g) => g.inject('  feature', ' one test', null, '', ' ', '　abc', 'abc　', '　abc　', '　　').lTrim(),
  ],
  'g_injectX__feature___test__nullX_lTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(['  feature  ', ' one test ', null, '', ' ', '　abc', 'abc　', '　abc　', '　　']).lTrim(scope.local),
  ],
  'g_injectX__feature__X_lTrim': <Function>[
    (GraphTraversalSource g) => g.inject('  feature  ').lTrim(),
  ],
  'g_injectXListXa_bXX_lTrim': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).lTrim(),
  ],
  'g_injectXListX1_2XX_lTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).lTrim(scope.local),
  ],
  'g_V_valuesXnameX_lTrim': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').lTrim(),
  ],
  'g_V_valuesXnameX_order_fold_lTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').order().fold().lTrim(scope.local),
  ],
  'g_injectXfeature_test_nullX_length': <Function>[
    (GraphTraversalSource g) => g.inject('feature', 'test', null).length(),
  ],
  'g_injectXfeature_test_nullX_lengthXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject('feature', 'test', null).length(scope.local),
  ],
  'g_injectXListXa_bXX_length': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).length(),
  ],
  'g_V_valuesXnameX_length': <Function>[
    (GraphTraversalSource g) => g.V().values('name').length(),
  ],
  'g_V_valuesXnameX_order_fold_lengthXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().fold().length(scope.local),
  ],
  'g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.both().simplePath()).until(Anon.has('name', 'peter').or_().loops().is_(GInt(3))).has('name', 'peter').path().by('name'),
  ],
  'g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.both().simplePath()).until(Anon.has('name', 'peter').or_().loops().is_(GInt(2))).has('name', 'peter').path().by('name'),
  ],
  'g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).repeat(Anon.both().simplePath()).until(Anon.has('name', 'peter').and_().loops().is_(GInt(3))).has('name', 'peter').path().by('name'),
  ],
  'g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().emit(Anon.has('name', 'marko').or_().loops().is_(GInt(2))).repeat(Anon.out()).values('name'),
  ],
  'g_VX1X_mapXvaluesXnameXX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).map_(Anon.values('name')),
  ],
  'g_VX1X_outE_label_mapXlengthX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().label().map_(Anon.length()),
  ],
  'g_VX1X_out_mapXvaluesXnameXX_mapXlengthX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().map_(Anon.values('name')).map_(Anon.length()),
  ],
  'g_withPath_V_asXaX_out_mapXselectXaX_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.withPath().V().as_('a').out().map_(Anon.select('a').values('name')),
  ],
  'g_withPath_V_asXaX_out_out_asXbX_mapXselectXaX_valuesXnameX_concatXselectXbX_valuesXnameXXX': <Function>[
    (GraphTraversalSource g) => g.withPath().V().as_('a').out().out().as_('b').map_(Anon.select('a').values('name').concat(Anon.select('b').values('name'))),
  ],
  'g_V_mapXselectXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').map_(Anon.select('a')),
  ],
  'g_V_mapXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().map_(Anon.constant(null)),
  ],
  'g_V_valueMap_matchXa_selectXnameX_bX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().match_(Anon.as_('a').select('name').as_('b')),
  ],
  'g_V_matchXa_out_bX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out().as_('b')),
  ],
  'g_V_matchXa_out_bX_selectXb_idX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out().as_('b')).select('b').by(t.id),
  ],
  'g_V_matchXa_knows_b__b_created_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('b').out('created').as_('c')),
  ],
  'g_V_matchXb_created_c__a_knows_bX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('b').out('created').as_('c'), Anon.as_('a').out('knows').as_('b')),
  ],
  'g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('created').as_('b'), Anon.as_('b').in_('created').as_('c')).where('a', P.neq('c')).select('a', 'c'),
  ],
  'g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('d').in_('knows').as_('a'), Anon.as_('d').has('name', 'vadas'), Anon.as_('a').out('knows').as_('b'), Anon.as_('b').out('created').as_('c')),
  ],
  'g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('created').has('name', 'lop').as_('b'), Anon.as_('b').in_('created').has('age', GInt(29)).as_('c'), Anon.as_('c').where(Anon.repeat(Anon.out()).times(GInt(2)))),
  ],
  'g_V_asXaX_out_asXbX_matchXa_out_count_c__b_in_count_cX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').match_(Anon.as_('a').out().count().as_('c'), Anon.as_('b').in_().count().as_('c')),
  ],
  'g_V_matchXa__a_out_b__notXa_created_bXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out().as_('b'), Anon.not_(Anon.as_('a').out('created').as_('b'))),
  ],
  'g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('created').has('name', 'lop').as_('b'), Anon.as_('b').in_('created').has('age', GInt(29)).as_('c')).where(Anon.as_('c').repeat(Anon.out()).times(GInt(2))).select('a', 'b', 'c'),
  ],
  'g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name': <Function>[
    (GraphTraversalSource g) => g.V().out().out().match_(Anon.as_('a').in_('created').as_('b'), Anon.as_('b').in_('knows').as_('c')).select('c').out('created').values('name'),
  ],
  'g_V_matchXa_knows_b__b_created_c__a_created_cX_dedupXa_b_cX_selectXaX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('b').out('created').as_('c'), Anon.as_('a').out('created').as_('c')).dedup('a', 'b', 'c').select('a').by('name'),
  ],
  'g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('created').as_('b'), Anon.as_('a').repeat(Anon.out()).times(GInt(2)).as_('b')).select('a', 'b'),
  ],
  'g_V_notXmatchXa_age_b__a_name_cX_whereXb_eqXcXX_selectXaXX_name': <Function>[
    (GraphTraversalSource g) => g.V().not_(Anon.match_(Anon.as_('a').values('age').as_('b'), Anon.as_('a').values('name').as_('c')).where('b', P.eq('c')).select('a')).values('name'),
  ],
  'g_V_matchXa_knows_b__andXa_created_c__b_created_c__andXb_created_count_d__a_knows_count_dXXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').as_('b'), Anon.and_(Anon.as_('a').out('created').as_('c'), Anon.as_('b').out('created').as_('c'), Anon.and_(Anon.as_('b').out('created').count().as_('d'), Anon.as_('a').out('knows').count().as_('d')))),
  ],
  'g_V_matchXa_whereXa_neqXcXX__a_created_b__orXa_knows_vadas__a_0knows_and_a_hasXlabel_personXX__b_0created_c__b_0created_count_isXgtX1XXX_selectXa_b_cX_byXidX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.where('a', P.neq('c')), Anon.as_('a').out('created').as_('b'), Anon.or_(Anon.as_('a').out('knows').has('name', 'vadas'), Anon.as_('a').in_('knows').and_().as_('a').has(t.label, 'person')), Anon.as_('b').in_('created').as_('c'), Anon.as_('b').in_('created').count().is_(P.gt(GInt(1)))).select('a', 'b', 'c').by(t.id),
  ],
  'g_V_matchXa__a_both_b__b_both_cX_dedupXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').both().as_('b'), Anon.as_('b').both().as_('c')).dedup('a', 'b'),
  ],
  'g_V_matchXa_knows_b__b_created_lop__b_matchXb_created_d__d_0created_cX_selectXcX_cX_selectXa_b_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('b').out('created').has('name', 'lop'), Anon.as_('b').match_(Anon.as_('b').out('created').as_('d'), Anon.as_('d').in_('created').as_('c')).select('c').as_('c')).select('a', 'b', 'c'),
  ],
  'g_V_matchXa_knows_b__a_created_cX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').as_('b'), Anon.as_('a').out('created').as_('c')),
  ],
  'g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.where(Anon.and_(Anon.as_('a').out('created').as_('b'), Anon.as_('b').in_('created').count().is_(P.eq(GInt(3))))), Anon.as_('a').both().as_('b'), Anon.where(Anon.as_('b').in_())),
  ],
  'g_V_matchXa_outEXcreatedX_order_byXweight_descX_limitX1X_inV_b__b_hasXlang_javaXX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').outE('created').order().by('weight', order.desc).limit(GInt(1)).inV().as_('b'), Anon.as_('b').has('lang', 'java')).select('a', 'b').by('name'),
  ],
  'g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').both().as_('b'), Anon.as_('b').both().as_('c')).dedup('a', 'b').by(t.label),
  ],
  'g_V_matchXa_created_b__b_0created_aX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('created').as_('b'), Anon.as_('b').in_('created').as_('a')),
  ],
  'g_V_asXaX_out_asXbX_matchXa_out_count_c__orXa_knows_b__b_in_count_c__and__c_isXgtX2XXXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').match_(Anon.as_('a').out().count().as_('c'), Anon.or_(Anon.as_('a').out('knows').as_('b'), Anon.as_('b').in_().count().as_('c').and_().as_('c').is_(P.gt(GInt(2))))),
  ],
  'g_V_matchXa_knows_count_bX_selectXbX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').count().as_('b')).select('b'),
  ],
  'g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').in_('sungBy').as_('b'), Anon.as_('a').in_('writtenBy').as_('c'), Anon.as_('b').out('writtenBy').as_('d'), Anon.as_('c').out('sungBy').as_('d'), Anon.as_('d').has('name', 'Garcia')),
  ],
  'g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').has('song', 'name', 'HERE COMES SUNSHINE'), Anon.as_('a').map_(Anon.inE('followedBy').values('weight').mean()).as_('b'), Anon.as_('a').inE('followedBy').as_('c'), Anon.as_('c').filter_(Anon.values('weight').where(P.gte('b'))).outV().as_('d')).select('d').by('name'),
  ],
  'g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').in_('sungBy').as_('b'), Anon.as_('a').in_('sungBy').as_('c'), Anon.as_('b').out('writtenBy').as_('d'), Anon.as_('c').out('writtenBy').as_('e'), Anon.as_('d').has('name', 'George_Harrison'), Anon.as_('e').has('name', 'Bob_Marley')),
  ],
  'g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').has('name', 'Garcia'), Anon.as_('a').in_('writtenBy').as_('b'), Anon.as_('a').in_('sungBy').as_('b')),
  ],
  'g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('song').match_(Anon.as_('a').values('name').as_('b'), Anon.as_('a').values('performances').as_('c')).select('b', 'c').count(),
  ],
  'g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('followedBy').count().is_(P.gt(GInt(10))).as_('b'), Anon.as_('a').in_('followedBy').count().is_(P.gt(GInt(10))).as_('b')).count(),
  ],
  'g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').in_('sungBy').as_('b'), Anon.as_('a').in_('writtenBy').as_('c'), Anon.as_('b').out('writtenBy').as_('d')).where(Anon.as_('c').out('sungBy').as_('d')).where(Anon.as_('d').has('name', 'Garcia')),
  ],
  'g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').has('name', 'Garcia'), Anon.as_('a').in_('writtenBy').as_('b'), Anon.as_('b').out('followedBy').as_('c'), Anon.as_('c').out('writtenBy').as_('d'), Anon.where('d', P.neq('a'))),
  ],
  'g_V_matchXa_outXknowsX_name_bX_identity': <Function>[
    (GraphTraversalSource g) => g.V().match_(Anon.as_('a').out('knows').values('name').as_('b')).identity(),
  ],
  'g_V_outE_mathX0_minus_itX_byXweightX': <Function>[
    (GraphTraversalSource g) => g.V().outE().math_('0-_').by('weight'),
  ],
  'g_V_hasXageX_valueMap_mathXit_plus_itXbyXselectXageX_unfoldXX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').valueMap().math_('_+_').by(Anon.select('age').unfold()),
  ],
  'g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('knows').as_('b').math_('a + b').by('age'),
  ],
  'g_withSideEffectXx_100X_V_age_mathX__plus_xX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('x', GInt(100)).V().values('age').math_('_ + x'),
  ],
  'g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').as_('b').math_('b + a').by(Anon.in_('created').count()).by('age'),
  ],
  'g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(1)).inject(GInt(1)).repeat(Anon.sack(operator_.sum).by(Anon.constant(GInt(1)))).times(GInt(5)).emit().math_('sin _').by(Anon.sack()),
  ],
  'g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX': <Function>[
    (GraphTraversalSource g) => g.V().project('a', 'b', 'c').by(Anon.bothE().values('weight').sum()).by(Anon.bothE().count()).by('name').order().by(Anon.math_('a / b'), order.desc).select('c'),
  ],
  'g_V_mathXit_plus_itXbyXageX': <Function>[
    (GraphTraversalSource g) => g.V().math_('_+_').by('age'),
  ],
  'g_V_valueMap_mathXit_plus_itXbyXselectXageX_unfoldXX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().math_('_+_').by(Anon.select('age').unfold()),
  ],
  'g_VX1X_outE_asXexpectedWeightX_mathXexpectedWeightPlusOneXbyXweightX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().as_('expectedWeight').math_('expectedWeight + 1').by('weight'),
  ],
  'g_V_age_max': <Function>[
    (GraphTraversalSource g) => g.V().values('age').max(),
  ],
  'g_V_foo_max': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').max(),
  ],
  'g_V_name_max': <Function>[
    (GraphTraversalSource g) => g.V().values('name').max(),
  ],
  'g_V_age_fold_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().max(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').max(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').max(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_unfold_max': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').unfold().max(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_max': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').unfold().max(),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').max(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').max(scope.local),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_unfold_max': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').unfold().max(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_max': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').unfold().max(),
  ],
  'g_V_foo_fold_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').fold().max(scope.local),
  ],
  'g_V_name_fold_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().max(scope.local),
  ],
  'g_V_repeatXbothX_timesX5X_age_max': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both()).times(GInt(5)).values('age').max(),
  ],
  'g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').group().by('name').by(Anon.bothE().values('weight').max()),
  ],
  'g_VX1X_valuesXageX_maxXlocalX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').max(scope.local),
  ],
  'g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_maxXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.union(Anon.values('age'), Anon.outE().values('weight')).fold()).max(scope.local),
  ],
  'g_V_age_mean': <Function>[
    (GraphTraversalSource g) => g.V().values('age').mean(),
  ],
  'g_V_foo_mean': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').mean(),
  ],
  'g_V_age_fold_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().mean(scope.local),
  ],
  'g_V_foo_fold_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').fold().mean(scope.local),
  ],
  'g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').group().by('name').by(Anon.bothE().values('weight').mean()),
  ],
  'g_V_aggregateXaX_byXageX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').mean(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').mean(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_unfold_mean': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').unfold().mean(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_mean': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').unfold().mean(),
  ],
  'g_V_aggregateXaX_byXfooX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').mean(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').mean(scope.local),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_unfold_mean': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').unfold().mean(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_mean': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').unfold().mean(),
  ],
  'g_injectXnull_10_20_nullX_mean': <Function>[
    (GraphTraversalSource g) => g.inject(null, GInt(10), GInt(20), null).mean(),
  ],
  'g_injectXlistXnull_10_20_nullXX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([null, GInt(10), GInt(20), null]).mean(scope.local),
  ],
  'g_VX1X_valuesXageX_meanXlocalX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').mean(scope.local),
  ],
  'g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_meanXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.union(Anon.values('age'), Anon.outE().values('weight')).fold()).mean(scope.local),
  ],
  'g_injectXnullX_mergeXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).merge_(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_mergeXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').merge_(Anon.V().fold()),
  ],
  'g_V_fold_mergeXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().merge_(Anon.constant(null)),
  ],
  'g_V_fold_mergeXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().merge_(Anon.V()),
  ],
  'g_V_elementMap_mergeXconstantXaXX': <Function>[
    (GraphTraversalSource g) => g.V().elementMap().merge_(Anon.constant('a')),
  ],
  'g_V_fold_mergeXV_asXaX_projectXaX_byXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().merge_(Anon.V().as_('a').project('a').by('name')),
  ],
  'g_V_fold_mergeXk_vX': <Function>[
    (GraphTraversalSource g) => g.V().fold().merge_({'k': 'v'}),
  ],
  'g_V_valuesXnameX_fold_mergeX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().merge_(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_mergeXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().merge_(null),
  ],
  'g_V_valuesXnonexistantX_fold_mergeXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().merge_(Anon.V().values('name').fold()),
  ],
  'g_V_valuesXnameX_fold_mergeXV_valuesXnonexistantX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().merge_(Anon.V().values('nonexistant').fold()),
  ],
  'g_V_valuesXageX_fold_mergeXV_valuesXageX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().merge_(Anon.V().values('age').fold()),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_mergeXMARKOX': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).merge_(['MARKO']),
  ],
  'g_injectXmarkoX_mergeXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).merge_(Anon.V().values('name').fold()),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_mergeXseattle_vancouverX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().merge_(['seattle', 'vancouver']),
  ],
  'g_V_out_out_path_byXnameX_mergeXempty_listX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').merge_([]),
  ],
  'g_V_valuesXageX_fold_mergeXconstantX27X_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().merge_(Anon.constant(GInt(27)).fold()),
  ],
  'g_V_out_out_path_byXnameX_mergeXdave_kelvinX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').merge_(['dave', 'kelvin']),
  ],
  'g_injectXa_null_bX_mergeXa_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).merge_(['a', 'c']),
  ],
  'g_injectXa_null_bX_mergeXa_null_cX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).merge_(['a', null, 'c']),
  ],
  'g_injectX3_threeX_mergeXfive_three_7X': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).merge_(['five', 'three', GInt(7)]),
  ],
  'g_V_asXnameX_projectXnameX_byXnameX_mergeXother_blueprintX': <Function>[
    (GraphTraversalSource g) => g.V().as_('name').project('name').by('name').merge_({'other': 'blueprint'}),
  ],
  'g_V_hasXname_markoX_elementMap_mergeXV_hasXname_lopX_elementMapX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').elementMap().merge_(Anon.V().has('name', 'lop').elementMap()),
  ],
  'g_V_mergeEXlabel_selfX_optionXonMatch_emptyX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addE('self'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().mergeE(xx1).option(merge.onMatch, {}),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().properties(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_V_mergeEXlabel_selfX_optionXonMatch_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addE('self'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().mergeE(xx1).option(merge.onMatch, null),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().properties(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_V_mergeEXemptyX_optionXonCreate_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('v').mergeE(xx1).option(merge.onCreate, null).option(merge.outV, Anon.select('v')).option(merge.inV, Anon.select('v')),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_V_mergeE_inlineXemptyX_optionXonCreate_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.V().as_('v').mergeE({t.label: 'self', (direction.OUT): merge.outV, (direction.in_): merge.inV}).option(merge.onCreate, null).option(merge.outV, Anon.select('v')).option(merge.inV, Anon.select('v')),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeEXemptyX_exists': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addE('self'),
    (GraphTraversalSource g) => g.mergeE({}),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeEXemptyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeE({}),
  ],
  'g_V_mergeEXemptyX_two_exist': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().as_('v').mergeE(xx1).option(merge.outV, Anon.select('v')).option(merge.inV, Anon.select('v')),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_V_mergeE_inlineXemptyX_two_exist': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g) => g.V().as_('v').mergeE({t.label: 'self', (direction.OUT): merge.outV, (direction.in_): merge.inV}).option(merge.outV, Anon.select('v')).option(merge.inV, Anon.select('v')),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeEXnullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeE(null),
  ],
  'g_mergeEXnullvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
  ],
  'g_V_limitX1X_mergeEXnullvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().limit(GInt(1)).mergeE(xx1),
  ],
  'g_V_mergeEXnullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.V().mergeE(null),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_withSideEffectXa_label_knows_out_marko_in_vadasX_mergeEXselectXaXX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g) => g.mergeE(Anon.select('a')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_marko1_in_vadas1X': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').outE('knows').has('weight', GDouble(0.5)).inV().has('person', 'name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V().has('person', 'name', 'marko').mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_withSideEffectXlabel_knows_out_marko_in_vadasX_injectX1X_selectXmX_mergeE': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g) => g.inject(GInt(1)).select('m').mergeE(),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N').inV().has('name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N').outV().has('name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
  ],
  'g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated_error': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E().hasLabel('knows').has('created', 'N').outV().has('name', 'vadas'),
  ],
  'g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2).option(merge.onMatch, xx3),
  ],
  'g_withSideEffect_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated_dynamic_override_sketchily_allowed': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('created', 'Y').addE('knows').from_('b').to('a').property('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('sideEffect1')).option(merge.onMatch, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3}) => g.E().hasLabel('knows').has('created', 'N').outV().has('name', 'vadas'),
  ],
  'g_V_hasXperson_name_marko_X_mergeEXlabel_self_out_vadas1_in_vadas1X': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('self').bothV().has('name', 'vadas'),
  ],
  'g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX_exists': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('c')).option(merge.onMatch, Anon.select('m')),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('c')).option(merge.onMatch, Anon.select('m')),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko1_in_vadas1X_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('c')).option(merge.onMatch, Anon.select('m')),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'N'),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_aliased_direction': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
  ],
  'g_withSideEffectXm1_label_knows_out_marko_in_vadas_m2_label_self_out_vadas_in_vadasX_unionXselectXm1X_selectXm2XX_mergeE': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g) => g.union(Anon.select('m1'), Anon.select('m2')).mergeE(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').out('knows').has('person', 'name', 'vadas'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'vadas').out('self').has('person', 'name', 'vadas'),
  ],
  'g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_sideEffectXpropertiesXweightX_dropX_selectXmXX_exists': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').property('weight', GDouble(1.0)).from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('c')).option(merge.onMatch, Anon.sideEffect(Anon.properties('weight').drop()).select('m')),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'Y'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('created', 'N'),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('weight'),
  ],
  'g_mergeE_with_outVinV_options_map': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeE(xx1).option(merge.outV, xx2).option(merge.inV, xx3),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_inline_with_outVinV_options_map': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE({(direction.OUT): merge.outV, (direction.in_): merge.inV, t.label: 'knows'}).option(merge.outV, xx1).option(merge.inV, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_with_outVinV_options_select': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid2, dynamic vid1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid2, dynamic vid1}) => g.V(vid1).as_('x').V(vid2).as_('y').mergeE(xx1).option(merge.outV, Anon.select('x')).option(merge.inV, Anon.select('y')),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid2, dynamic vid1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid2, dynamic vid1}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_inline_with_outVinV_options_select': <Function>[
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(vid1).as_('x').V(vid2).as_('y').mergeE({(direction.OUT): merge.outV, (direction.in_): merge.inV, t.label: 'knows'}).option(merge.outV, Anon.select('x')).option(merge.inV, Anon.select('y')),
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V(),
    (GraphTraversalSource g, {dynamic vid2, dynamic vid1}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_with_eid_specified_and_inheritance_1': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E('201'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_with_eid_specified_and_inheritance_2': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E('201'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeE_outV_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
  ],
  'g_withSideEffect_withSideEffect_mergeE_outV_dynamic_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('sideEffect1')),
  ],
  'g_mergeE_inV_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
  ],
  'g_withSideEffect_mergeE_inV_dynamic_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('sideEffect1')),
  ],
  'g_mergeE_label_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
  ],
  'g_mergeE_label_dynamic_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('sideEffect1')),
  ],
  'g_mergeE_id_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onCreate, xx2),
  ],
  'g_withSideEffect_mergeE_id_dynamic_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onCreate, Anon.select('sideEffect1')),
  ],
  'g_mergeV_mergeE_combination_new_vertices': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeV(xx1).as_('outV').mergeV(xx2).as_('inV').mergeE(xx3).option(merge.outV, Anon.select('outV')).option(merge.inV, Anon.select('inV')),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_mergeV_mergeE_combination_existing_vertices': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.addV('person').property('name', 'marko').addV('person').property('name', 'vadas'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.mergeV(xx1).as_('outV').mergeV(xx2).as_('inV').mergeE(xx3).option(merge.outV, Anon.select('outV')).option(merge.inV, Anon.select('inV')),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx3, dynamic xx2}) => g.V().has('name', 'marko').out('knows').has('name', 'vadas'),
  ],
  'g_V_asXvX_mergeEXxx1X_optionXMerge_onMatch_xx2X_optionXMerge_outV_selectXvXX_optionXMerge_inV_selectXvXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().as_('v').mergeE(xx1).option(merge.onMatch, xx2).option(merge.outV, Anon.select('v')).option(merge.inV, Anon.select('v')),
  ],
  'g_V_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_sideEffectXpropertyXweight_0XX_constantXemptyXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').property('weight', GInt(1)).from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().mergeE(xx1).option(merge.onMatch, Anon.sideEffect(Anon.property('weight', GInt(0))).constant({})),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('weight', GInt(0)),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_sideEffectXpropertyXweight_0XX_constantXemptyXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').property('weight', GInt(1)).from_('a').to('b'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeE(xx1).option(merge.onMatch, Anon.sideEffect(Anon.property('weight', GInt(0))).constant({})),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('weight', GInt(1)),
    (GraphTraversalSource g, {dynamic xx1}) => g.E().hasLabel('knows').has('weight', GInt(0)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('weight'),
  ],
  'g_unionXselectXmapX_selectXmapX_constantXcreated_NXX_fold_asXmX_mergeEXselectXmX_limitXlocal_1X_unfoldX_optionXonCreate_selectXmX_rangeXlocal_1_2X_unfoldX_optionXonMatch_selectXmX_tailXlocalX_unfoldX_to_match': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.union(Anon.select('map'), Anon.select('map'), Anon.constant({'created': 'N'})).fold().as_('m').mergeE(Anon.select('m').limit(scope.local, GInt(1)).unfold()).option(merge.onCreate, Anon.select('m').range(scope.local, GInt(1), GInt(2)).unfold()).option(merge.onMatch, Anon.select('m').tail(scope.local).unfold()),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.E().has('created', 'N'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').outE('knows').has('created', 'N').inV().has('person', 'name', 'vadas'),
  ],
  'g_unionXselectXmapX_selectXmapX_constantXcreated_NXX_fold_asXmX_mergeEXselectXmX_limitXlocal_1X_unfoldX_optionXonCreate_selectXmX_rangeXlocal_1_2X_unfoldX_optionXonMatch_selectXmX_tailXlocalX_unfoldX_to_create': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.union(Anon.select('map'), Anon.select('map'), Anon.constant({'created': 'N'})).fold().as_('m').mergeE(Anon.select('m').limit(scope.local, GInt(1)).unfold()).option(merge.onCreate, Anon.select('m').range(scope.local, GInt(1), GInt(2)).unfold()).option(merge.onMatch, Anon.select('m').tail(scope.local).unfold()),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
    (GraphTraversalSource g) => g.E().hasNot('created'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').outE('knows').hasNot('created').inV().has('person', 'name', 'vadas'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'vadas').outE('self').hasNot('weight').inV().has('person', 'name', 'vadas'),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_weight_nullX_allowed': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('weight', GDouble(1.0)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E().hasLabel('knows'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E().hasLabel('knows').has('weight', null),
  ],
  'g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_weight_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').as_('a').addV('person').property('name', 'vadas').as_('b').addE('knows').from_('a').to('b').property('weight', GDouble(1.0)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeE(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E().hasLabel('knows'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.E().hasLabel('knows').has('weight'),
  ],
  'g_mergeVXemptyX_optionXonMatch_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV({}).option(merge.onMatch, null),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(29)),
  ],
  'g_V_mergeVXemptyX_optionXonMatch_nullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.V().mergeV({}).option(merge.onMatch, null),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(29)),
  ],
  'g_mergeVXnullX_optionXonCreate_label_null_name_markoX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
  ],
  'g_V_mergeVXnullX_optionXonCreate_label_null_name_markoX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().mergeV(xx1),
  ],
  'g_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1).option(merge.onCreate, null),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_V_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().mergeV(xx1).option(merge.onCreate, null),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_mergeVXnullX_optionXonCreate_emptyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV(null).option(merge.onCreate, {}),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_V_mergeVXnullX_optionXonCreate_emptyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.V().mergeV(null).option(merge.onCreate, {}),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeVXemptyX_no_existing': <Function>[
    (GraphTraversalSource g) => g.mergeV({}),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_injectX0X_mergeVXemptyX_no_existing': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(0)).mergeV({}),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeVXemptyX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV({}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(29)),
  ],
  'g_V_mergeVXemptyX_two_exist': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g) => g.V().mergeV({}),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(29)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'vadas').has('age', GInt(27)),
  ],
  'g_mergeVXnullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV(null),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeVXnullvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_V_mergeVXnullX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.V().mergeV(null),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeVXlabel_person_name_stephenX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_mergeVXlabel_person_name_markoX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko'),
  ],
  'g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'stephen').has('age', GInt(19)),
  ],
  'g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
  ],
  'g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV(Anon.select('c')).option(merge.onCreate, Anon.select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen').has('age', GInt(19)),
  ],
  'g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.mergeV(Anon.select('c')).option(merge.onMatch, Anon.select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
  ],
  'g_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1).property('name', 'vadas', 'acl', 'public'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().properties('name').hasValue('vadas').has('acl', 'public'),
  ],
  'g_injectX0X_mergeVXlabel_person_name_stephenX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.inject(GInt(0)).mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'stephen'),
  ],
  'g_injectX0X_mergeVXlabel_person_name_markoX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.inject(GInt(0)).mergeV(xx1),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'marko'),
  ],
  'g_injectX0X_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.inject(GInt(0)).mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'stephen').has('age', GInt(19)),
  ],
  'g_injectX0X_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.inject(GInt(0)).mergeV(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
  ],
  'g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_injectX0X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject(GInt(0)).mergeV(Anon.select('c')).option(merge.onCreate, Anon.select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen').has('age', GInt(19)),
  ],
  'g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_injectX0X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject(GInt(0)).mergeV(Anon.select('c')).option(merge.onMatch, Anon.select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
  ],
  'g_injectX0X_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g, {dynamic xx1}) => g.inject(GInt(0)).mergeV(xx1).property('name', 'vadas', 'acl', 'public'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().properties('name').hasValue('vadas').has('acl', 'public'),
  ],
  'g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeVXidentityX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject({t.label: 'person', 'name': 'marko'}, {t.label: 'person', 'name': 'stephen'}).mergeV(Anon.identity()),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeV': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject({t.label: 'person', 'name': 'marko'}, {t.label: 'person', 'name': 'stephen'}).mergeV(),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_mergeVXlabel_person_name_stephenX_propertyXlist_name_steveX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property(cardinality.list, 'name', 'stephen'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1).property(cardinality.list, 'name', 'steve'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().properties('name').hasValue('steve'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().properties('name').hasValue('stephen'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().properties('name'),
  ],
  'g_mergeXlabel_person_name_vadasX_optionXonMatch_age_35X': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'vadas').property('age', GInt(29)).addV('person').property('name', 'vadas').property('age', GInt(27)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onMatch, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V().has('age', GInt(35)),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
  ],
  'g_V_mapXmergeXlabel_person_name_joshXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('person').property('name', 'vadas').property('age', GInt(29)).addV('person').property('name', 'stephen').property('age', GInt(27)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().map_(Anon.mergeV(xx1)),
    (GraphTraversalSource g, {dynamic xx1}) => g.V().has('person', 'name', 'josh'),
    (GraphTraversalSource g, {dynamic xx1}) => g.V(),
  ],
  'g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV(Anon.select('c')).option(merge.onMatch, Anon.sideEffect(Anon.properties('age').drop()).select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
  ],
  'g_withSideEffectXm_age_19X_V_hasXperson_name_markoX_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').mergeV({}).option(merge.onMatch, Anon.sideEffect(Anon.properties('age').drop()).select('m')),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(19)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeV_onCreate_inheritance_existing': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.addV('person').property('name', 'mike').property(t.id, '1'),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V('1').has('person', 'name', 'mike'),
  ],
  'g_mergeV_onCreate_inheritance_new_1': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V('1').has('person', 'name', 'mike'),
  ],
  'g_mergeV_onCreate_inheritance_new_2': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V(),
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.V('1').has('person', 'name', 'mike'),
  ],
  'g_mergeV_label_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
  ],
  'g_withSideEffect_mergeV_label_dynamic_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1).option(merge.onCreate, Anon.select('sideEffect1')),
  ],
  'g_mergeV_id_override_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic xx2}) => g.mergeV(xx1).option(merge.onCreate, xx2),
  ],
  'g_mergeV_hidden_id_key_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
  ],
  'g_mergeV_hidden_label_key_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
  ],
  'g_mergeV_hidden_label_value_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV(xx1),
  ],
  'g_mergeV_hidden_id_key_onCreate_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onCreate, xx1),
  ],
  'g_mergeV_hidden_label_key_onCreate_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onCreate, xx1),
  ],
  'g_mergeV_hidden_label_value_onCreate_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onCreate, xx1),
  ],
  'g_mergeV_hidden_id_key_onMatch_matched_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('vertex'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onMatch, xx1),
  ],
  'g_mergeV_hidden_label_key_matched_onMatch_matched_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.addV('vertex'),
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onMatch, xx1),
  ],
  'g_mergeVXname_markoX_optionXonMatch_age_listX33XX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'age': cardinality.list(GInt(33))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(33)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_age_setX33XX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'age': cardinality.set_(GInt(33))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(33)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_age_setX31XX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'age': cardinality.set_(GInt(31))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(31)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_age_singleX33XX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'age': cardinality.single(GInt(33))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(33)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_age_33_singleX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'age': GInt(33)}, cardinality.single),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age', GInt(33)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_name_allen_age_setX31X_singleX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'name': 'allen', 'age': cardinality.set_(GInt(31))}, cardinality.single),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').has('age', GInt(31)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').properties('age'),
  ],
  'g_mergeVXname_markoX_optionXonMatch_name_allen_age_singleX31X_singleX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property(cardinality.list, 'age', GInt(29)).property(cardinality.list, 'age', GInt(31)).property(cardinality.list, 'age', GInt(32)),
    (GraphTraversalSource g) => g.mergeV({'name': 'marko'}).option(merge.onMatch, {'name': 'allen', 'age': cardinality.single(GInt(31))}, cardinality.single),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').has('age', GInt(33)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').has('age', GInt(31)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'allen').properties('age'),
  ],
  'g_mergeVXname_aliceX_optionXonCreate_age_singleX81XX': <Function>[
    (GraphTraversalSource g) => g.mergeV({'name': 'alice', (t.label): 'person'}).option(merge.onCreate, {'age': cardinality.single(GInt(81))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age', GInt(81)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').properties('age'),
  ],
  'g_mergeVXname_aliceX_optionXonCreate_age_setX81XX': <Function>[
    (GraphTraversalSource g) => g.mergeV({'name': 'alice', (t.label): 'person'}).option(merge.onCreate, {'age': cardinality.set_(GInt(81))}),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age', GInt(81)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').properties('age'),
  ],
  'g_mergeVXname_aliceX_optionXonCreate_age_81_setX': <Function>[
    (GraphTraversalSource g) => g.mergeV({'name': 'alice', (t.label): 'person'}).option(merge.onCreate, {'age': GInt(81)}, cardinality.set_),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age', GInt(81)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').properties('age'),
  ],
  'g_mergeVXname_aliceX_optionXonCreate_age_81_label_person_setX': <Function>[
    (GraphTraversalSource g) => g.mergeV({'name': 'alice'}).option(merge.onCreate, {'age': GInt(81), (t.label): 'person'}, cardinality.set_),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age', GInt(81)),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').has('age'),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'alice').properties('age'),
  ],
  'g_mergeV_hidden_label_key_onMatch_matched_prohibited': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.mergeV({}).option(merge.onMatch, xx1),
  ],
  'g_injectXlist1_list2_list3X_fold_asXmX_mergeVXselectXmX_limitXlocal_1X_unfoldX_optionXonCreate_selectXmX_rangeXlocal_1_2X_unfoldX_optionXonMatch_selectXmX_tailXlocalX_unfoldX_to_match': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject({t.label: 'person', 'name': 'marko'}, {t.label: 'person', 'name': 'marko'}, {'created': 'N'}).fold().as_('m').mergeV(Anon.select('m').limit(scope.local, GInt(1)).unfold()).option(merge.onCreate, Anon.select('m').range(scope.local, GInt(1), GInt(2)).unfold()).option(merge.onMatch, Anon.select('m').tail(scope.local).unfold()),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').has('created', 'N'),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_injectXlist1_list2_list3X_fold_asXmX_mergeVXselectXmX_limitXlocal_1X_unfoldX_optionXonCreate_selectXmX_rangeXlocal_1_2X_unfoldX_optionXonMatch_selectXmX_tailXlocalX_unfoldX_to_create': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'marko').property('age', GInt(29)),
    (GraphTraversalSource g) => g.inject({t.label: 'person', 'name': 'stephen'}, {t.label: 'person', 'name': 'stephen'}, {'created': 'N'}).fold().as_('m').mergeV(Anon.select('m').limit(scope.local, GInt(1)).unfold()).option(merge.onCreate, Anon.select('m').range(scope.local, GInt(1), GInt(2)).unfold()).option(merge.onMatch, Anon.select('m').tail(scope.local).unfold()),
    (GraphTraversalSource g) => g.V().has('person', 'name', 'stephen').hasNot('created'),
    (GraphTraversalSource g) => g.V(),
  ],
  'g_V_age_min': <Function>[
    (GraphTraversalSource g) => g.V().values('age').min(),
  ],
  'g_V_foo_min': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').min(),
  ],
  'g_V_name_min': <Function>[
    (GraphTraversalSource g) => g.V().values('name').min(),
  ],
  'g_V_age_fold_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().min(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').min(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').min(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_unfold_min': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').unfold().min(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_min': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').unfold().min(),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').min(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').min(scope.local),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_unfold_min': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').unfold().min(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_min': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').unfold().min(),
  ],
  'g_V_foo_fold_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').fold().min(scope.local),
  ],
  'g_V_name_fold_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().min(scope.local),
  ],
  'g_V_repeatXbothX_timesX5X_age_min': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both()).times(GInt(5)).values('age').min(),
  ],
  'g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').group().by('name').by(Anon.bothE().values('weight').min()),
  ],
  'g_V_foo_injectX9999999999X_min': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').inject(GLong(9999999999)).min(),
  ],
  'g_VX1X_valuesXageX_minXlocalX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').min(scope.local),
  ],
  'g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_minXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.union(Anon.values('age'), Anon.outE().values('weight')).fold()).min(scope.local),
  ],
  'g_V_name_order': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order(),
  ],
  'g_V_order_byXname_ascX_name': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name', order.asc).values('name'),
  ],
  'g_V_order_byXnameX_name': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name').values('name'),
  ],
  'g_V_outE_order_byXweight_descX_weight': <Function>[
    (GraphTraversalSource g) => g.V().outE().order().by('weight', order.desc).values('weight'),
  ],
  'g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('created').as_('b').order().by(order.shuffle).select('a', 'b'),
  ],
  'g_V_both_hasLabelXpersonX_order_byXage_descX_limitX5X_name': <Function>[
    (GraphTraversalSource g) => g.V().both().hasLabel('person').order().by('age', order.desc).limit(GInt(5)).values('name'),
  ],
  'g_V_properties_order_byXkey_descX_key': <Function>[
    (GraphTraversalSource g) => g.V().properties().order().by(t.key_, order.desc).key_(),
  ],
  'g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_orderXlocalX_byXvaluesX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').group().by('name').by(Anon.outE().values('weight').sum()).order(scope.local).by(column.values),
  ],
  'g_V_mapXbothE_weight_foldX_order_byXsumXlocalX_descX_byXcountXlocalX_descX': <Function>[
    (GraphTraversalSource g) => g.V().map_(Anon.bothE().values('weight').order().by(order.asc).fold()).order().by(Anon.sum(scope.local), order.desc).by(Anon.count(scope.local), order.desc),
  ],
  'g_V_group_byXlabelX_byXname_order_byXdescX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(t.label).by(Anon.values('name').order().by(order.desc).fold()),
  ],
  'g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_descX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').group().by('name').by(Anon.outE().values('weight').sum()).unfold().order().by(column.values, order.desc),
  ],
  'g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_descX_byXselectXvX_nameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('v').map_(Anon.bothE().values('weight').fold()).sum(scope.local).as_('s').select('v', 's').order().by(Anon.select('s'), order.desc).by(Anon.select('v').values('name')),
  ],
  'g_V_hasLabelXpersonX_fold_orderXlocalX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').fold().order(scope.local).by('age'),
  ],
  'g_V_both_hasLabelXpersonX_order_byXage_descX_name': <Function>[
    (GraphTraversalSource g) => g.V().both().hasLabel('person').order().by('age', order.desc).values('name'),
  ],
  'g_V_order_byXoutE_count_descX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().order().by(Anon.outE().count(), order.desc).by('name'),
  ],
  'g_V_hasLabelXpersonX_order_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').order().by('age'),
  ],
  'g_V_order_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().order().by('age'),
  ],
  'g_V_fold_orderXlocalX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().fold().order(scope.local).by('age'),
  ],
  'g_V_fold_orderXlocalX_byXage_descX': <Function>[
    (GraphTraversalSource g) => g.V().fold().order(scope.local).by('age', order.desc),
  ],
  'g_V_orXhasLabelXpersonX_hasXsoftware_name_lopXX_order_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().or_(Anon.hasLabel('person'), Anon.has('software', 'name', 'lop')).order().by('age'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_orXhasLabelXpersonX_hasXsoftware_name_lopXX_order_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().or_(Anon.hasLabel('person'), Anon.has('software', 'name', 'lop')).order().by('age'),
  ],
  'g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX': <Function>[
    (GraphTraversalSource g) => g.V().has('song', 'name', 'OH BOY').out('followedBy').out('followedBy').order().by('performances').by('songType', order.desc).by('name'),
  ],
  'g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('song').order().by('performances', order.desc).by('name').range(GInt(110), GInt(120)).values('name'),
  ],
  'g_VX1X_elementMap_orderXlocalX_byXkeys_descXunfold': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).elementMap().order(scope.local).by(column.keys, order.desc).unfold(),
  ],
  'g_VX1X_elementMap_orderXlocalX_byXkeys_ascXunfold': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).elementMap().order(scope.local).by(column.keys, order.asc).unfold(),
  ],
  'g_VX1X_valuesXageX_orderXlocalX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').order(scope.local),
  ],
  'g_V_pageRank_hasXpageRankX': <Function>[
    (GraphTraversalSource g) => g.V().pageRank().has('gremlin.pageRankVertexProgram.pageRank'),
  ],
  'g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankX_withXtimes_0X_valueMapXname_projectRankX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').pageRank().with_('~tinkerpop.pageRank.edges', Anon.bothE()).with_('~tinkerpop.pageRank.propertyName', 'projectRank').with_('~tinkerpop.pageRank.times', GInt(0)).valueMap('name', 'projectRank'),
  ],
  'g_V_pageRank_order_byXpageRank_descX_byXnameX_name': <Function>[
    (GraphTraversalSource g) => g.V().pageRank().order().by('gremlin.pageRankVertexProgram.pageRank', order.desc).by('name').values('name'),
  ],
  'g_V_pageRank_order_byXpageRank_descX_name_limitX2X': <Function>[
    (GraphTraversalSource g) => g.V().pageRank().order().by('gremlin.pageRankVertexProgram.pageRank', order.desc).values('name').limit(GInt(2)),
  ],
  'g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX': <Function>[
    (GraphTraversalSource g) => g.V().pageRank().with_('~tinkerpop.pageRank.edges', Anon.outE('knows')).with_('~tinkerpop.pageRank.propertyName', 'friendRank').project('name', 'friendRank').by('name').by(Anon.values('friendRank').math_('ceil(_ * 100)')),
  ],
  'g_V_hasLabelXpersonX_pageRank_withXpropertyName_kpageRankX_project_byXnameX_byXvaluesXpageRankX_mathX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').pageRank().with_('~tinkerpop.pageRank.propertyName', 'pageRank').project('name', 'pageRank').by('name').by(Anon.values('pageRank').math_('ceil(_ * 100)')),
  ],
  'g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX': <Function>[
    (GraphTraversalSource g) => g.V().pageRank().with_('~tinkerpop.pageRank.propertyName', 'pageRank').as_('a').out('knows').values('pageRank').as_('b').select('a', 'b').by().by(Anon.math_('ceil(_ * 100)')),
  ],
  'g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').has('name', 'ripple').pageRank(GDouble(1.0)).with_('~tinkerpop.pageRank.edges', Anon.inE('created')).with_('~tinkerpop.pageRank.times', GInt(1)).with_('~tinkerpop.pageRank.propertyName', 'priors').in_('created').union(Anon.both(), Anon.identity()).valueMap('name', 'priors'),
  ],
  'g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXpropertyName_pageRankX_withXedges_inEX_withXtimes_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').group('m').by(t.label).pageRank(GDouble(1.0)).with_('~tinkerpop.pageRank.propertyName', 'pageRank').with_('~tinkerpop.pageRank.edges', Anon.inE()).with_('~tinkerpop.pageRank.times', GInt(1)).in_('created').group('m').by('pageRank').cap('m'),
  ],
  'g_VX1X_name_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('name').path(),
  ],
  'g_VX1X_out_path_byXageX_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().path().by('age').by('name'),
  ],
  'g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out()).times(GInt(2)).path().by().by('name').by('lang'),
  ],
  'g_V_out_out_path_byXnameX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').by('age'),
  ],
  'g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').has('name', 'marko').as_('b').has('age', GInt(29)).as_('c').path(),
  ],
  'g_VX1X_outEXcreatedX_inV_inE_outV_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('created').inV().inE().outV().path(),
  ],
  'g_V_asXaX_out_asXbX_out_asXcX_path_fromXbX_toXcX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').out().as_('c').path().from_('b').to('c').by('name'),
  ],
  'g_VX1X_out_path_byXageX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().path().by('age'),
  ],
  'g_withStrategiesXProductiveByStrategyX_VX1X_out_path_byXageX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withStrategies(ProductiveByStrategy()).V(vid1).out().path().by('age'),
  ],
  'g_injectX1_null_nullX_path': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1), null, null).path(),
  ],
  'g_injectX1_null_nullX_path_dedup': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1), null, null).path().dedup(),
  ],
  'g_V_peerPressure_hasXclusterX': <Function>[
    (GraphTraversalSource g) => g.V().peerPressure().has('gremlin.peerPressureVertexProgram.cluster'),
  ],
  'g_V_peerPressure_withXpropertyName_clusterX_withXedges_outEXknowsXX_pageRankX1X_byXrankX_withXedges_outEXknowsX_withXtimes_2X_group_byXclusterX_byXrank_sumX_limitX100X': <Function>[
    (GraphTraversalSource g) => g.V().peerPressure().with_('~tinkerpop.peerPressure.propertyName', 'cluster').with_('~tinkerpop.peerPressure.edges', Anon.outE('knows')).pageRank(GDouble(1.0)).with_('~tinkerpop.pageRank.propertyName', 'rank').with_('~tinkerpop.pageRank.edges', Anon.outE('knows')).with_('~tinkerpop.pageRank.times', GInt(1)).group().by('cluster').by(Anon.values('rank').sum()).limit(GInt(100)),
  ],
  'g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXedges_outEX_withyXpropertyName_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'ripple').in_('created').peerPressure().with_('~tinkerpop.peerPressure.edges', Anon.outE()).with_('~tinkerpop.peerPressure.propertyName', 'cluster').repeat(Anon.union(Anon.identity(), Anon.both())).times(GInt(2)).dedup().valueMap('name', 'cluster'),
  ],
  'g_injectXnullX_productXinjectX1XX': <Function>[
    (GraphTraversalSource g) => g.inject(null).product(Anon.inject(GInt(1))),
  ],
  'g_V_valuesXnameX_productXV_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').product(Anon.V().fold()),
  ],
  'g_V_fold_productXconstantXnullXX': <Function>[
    (GraphTraversalSource g) => g.V().fold().product(Anon.constant(null)),
  ],
  'g_V_fold_productXVX': <Function>[
    (GraphTraversalSource g) => g.V().fold().product(Anon.V()),
  ],
  'g_V_valuesXnameX_fold_productX2X': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().product(GInt(2)),
  ],
  'g_V_valuesXnameX_fold_productXnullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().product(null),
  ],
  'g_V_valuesXnonexistantX_fold_productXV_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('nonexistant').fold().product(Anon.V().values('name').fold()),
  ],
  'g_V_valuesXnameX_fold_productXV_valuesXnonexistantX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().product(Anon.V().values('nonexistant').fold()),
  ],
  'g_V_valuesXageX_order_byXdescX_limitX3X_fold_productXV_valuesXageX_order_byXascX_limitX2X_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.desc).limit(GInt(3)).fold().product(Anon.V().values('age').order().by(order.asc).limit(GInt(2)).fold()).unfold(),
  ],
  'g_V_out_path_byXvaluesXnameX_toUpperX_productXMARKOX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by(Anon.values('name').toUpper()).product(['MARKO']).unfold(),
  ],
  'g_injectXmarkoX_productXV_valuesXnameX_order_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.inject(['marko']).product(Anon.V().values('name').order().fold()).unfold(),
  ],
  'g_V_valueMapXlocationX_selectXvaluesX_unfold_productXdulles_seattle_vancouverX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('location').select(column.values).unfold().product(['dulles', 'seattle', 'vancouver']).unfold(),
  ],
  'g_V_valuesXageX_order_byXascX_fold_productXconstantX27X_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().values('age').order().by(order.asc).fold().product(Anon.constant(GInt(27)).fold()).unfold(),
  ],
  'g_V_out_out_path_byXnameX_productXdave_kelvinX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').product(['dave', 'kelvin']).unfold(),
  ],
  'g_injectXa_null_bX_productXa_cX_unfold': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).product(['a', 'c']).unfold(),
  ],
  'g_injectXa_null_bX_productXa_null_cX_unfold': <Function>[
    (GraphTraversalSource g) => g.inject(['a', null, 'b']).product(['a', null, 'c']).unfold(),
  ],
  'g_injectX3_threeX_productXfive_three_7X_unfold': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).product(['five', 'three', GInt(7)]).unfold(),
  ],
  'g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').project('a', 'b').by(Anon.outE().count()).by('age'),
  ],
  'g_V_outXcreatedX_projectXa_bX_byXnameX_byXinXcreatedX_countX_order_byXselectXbX__descX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').project('a', 'b').by('name').by(Anon.in_('created').count()).order().by(Anon.select('b'), order.desc).select('a'),
  ],
  'g_V_valueMap_projectXxX_byXselectXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().project('x').by(Anon.select('name')),
  ],
  'g_V_projectXa_bX_byXinE_countX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().project('a', 'b').by(Anon.inE().count()).by('age'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_projectXa_bX_byXinE_countX_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().project('a', 'b').by(Anon.inE().count()).by('age'),
  ],
  'g_V_hasXageX_propertiesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').properties('name').value_(),
  ],
  'g_V_hasXageX_propertiesXname_ageX_value': <Function>[
    (GraphTraversalSource g) => g.V().has('age').properties('name', 'age').value_(),
  ],
  'g_V_hasXageX_propertiesXage_nameX_value': <Function>[
    (GraphTraversalSource g) => g.V().has('age').properties('age', 'name').value_(),
  ],
  'g_V_propertiesXname_age_nullX_value': <Function>[
    (GraphTraversalSource g) => g.V().properties('name', 'age', null).value_(),
  ],
  'g_V_valuesXname_age_nullX': <Function>[
    (GraphTraversalSource g) => g.V().values('name', 'age', null),
  ],
  'g_E_propertiesXweightX': <Function>[
    (GraphTraversalSource g) => g.E().properties('weight'),
  ],
  'g_E_properties': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addV('person').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('weight', GDouble(0.5)).property('since', GInt(2020)),
    (GraphTraversalSource g) => g.E().properties(),
  ],
  'g_E_propertiesXsinceX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addV('person').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('weight', GDouble(0.5)).property('since', GInt(2020)),
    (GraphTraversalSource g) => g.E().properties('since'),
  ],
  'g_E_properties_multi_edges': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addV('person').property('name', 'bob').as_('b').addE('knows').from_('a').to('b').property('weight', GDouble(0.5)).property('since', GInt(2020)).addE('likes').from_('a').to('b').property('weight', GDouble(1.0)).property('tag', 'friend'),
    (GraphTraversalSource g) => g.E().properties(),
  ],
  'g_injectX__feature___test__nullX_rTrim': <Function>[
    (GraphTraversalSource g) => g.inject('feature  ', 'one test ', null, '', ' ', '　abc', 'abc　', '　abc　', '　　').rTrim(),
  ],
  'g_injectX__feature___test__nullX_rTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(['  feature  ', ' one test ', null, '', ' ', '　abc', 'abc　', '　abc　', '　　']).rTrim(scope.local),
  ],
  'g_injectX__feature__X_rTrim': <Function>[
    (GraphTraversalSource g) => g.inject('  feature  ').rTrim(),
  ],
  'g_injectXListXa_bXX_rTrim': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).rTrim(),
  ],
  'g_injectXListX1_2XX_rTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).rTrim(scope.local),
  ],
  'g_V_valuesXnameX_rTrim': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').rTrim(),
  ],
  'g_V_valuesXnameX_order_fold_rTrimXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').order().fold().rTrim(scope.local),
  ],
  'g_injectXthat_this_test_nullX_replaceXh_jX': <Function>[
    (GraphTraversalSource g) => g.inject('that', 'this', 'test', null).replace('h', 'j'),
  ],
  'g_injectXthat_this_test_nullX_fold_replaceXlocal_h_jX': <Function>[
    (GraphTraversalSource g) => g.inject('that', 'this', 'test', null).fold().replace(scope.local, 'h', 'j'),
  ],
  'g_injectXListXa_bXcX_replaceXa_bX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).replace('a', 'b'),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_replaceXnull_iX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').replace(null, 'g'),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_replaceXa_iX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').replace('p', 'g'),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_order_fold_replaceXloacl_a_iX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').order().fold().replace(scope.local, 'p', 'g'),
  ],
  'g_injectXfeature_test_nullX_reverse': <Function>[
    (GraphTraversalSource g) => g.inject('feature', 'test one', null).reverse(),
  ],
  'g_V_valuesXnameX_reverse': <Function>[
    (GraphTraversalSource g) => g.V().values('name').reverse(),
  ],
  'g_V_valuesXageX_reverse': <Function>[
    (GraphTraversalSource g) => g.V().values('age').reverse(),
  ],
  'g_V_out_path_byXnameX_reverse': <Function>[
    (GraphTraversalSource g) => g.V().out().path().by('name').reverse(),
  ],
  'g_V_out_out_path_byXnameX_reverse': <Function>[
    (GraphTraversalSource g) => g.V().out().out().path().by('name').reverse(),
  ],
  'g_V_valuesXageX_fold_orderXlocalX_byXdescX_reverse': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().order(scope.local).by(order.desc).reverse(),
  ],
  'g_V_valuesXnameX_fold_orderXlocalX_by_reverse': <Function>[
    (GraphTraversalSource g) => g.V().values('name').fold().order(scope.local).by().reverse(),
  ],
  'g_injectXnullX_reverse': <Function>[
    (GraphTraversalSource g) => g.inject(null).reverse(),
  ],
  'g_injectXbX_reverse': <Function>[
    (GraphTraversalSource g) => g.inject('b').reverse(),
  ],
  'g_injectX3_threeX_reverse': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(3), 'three']).reverse(),
  ],
  'g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('knows').as_('b').select('a', 'b'),
  ],
  'g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('knows').as_('b').select('a', 'b').by('name'),
  ],
  'g_VX1X_asXaX_outXknowsX_asXbX_selectXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('knows').as_('b').select('a'),
  ],
  'g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').out('knows').as_('b').select('a').by('name'),
  ],
  'g_V_asXaX_out_asXbX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('b').select('a', 'b').by('name'),
  ],
  'g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().aggregate('x').as_('b').select('a', 'b').by('name'),
  ],
  'g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').values('name').order().as_('b').select('a', 'b').by('name').by(),
  ],
  'g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'gremlin').inE('uses').order().by('skill', order.asc).as_('a').outV().as_('b').select('a', 'b').by('skill').by('name'),
  ],
  'g_V_whereX_valueXnameX_isXmarkoXX_asXaX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().where(Anon.values('name').is_('marko')).as_('a').select('a'),
  ],
  'g_V_label_groupCount_asXxX_selectXxX': <Function>[
    (GraphTraversalSource g) => g.V().label().groupCount().as_('x').select('x'),
  ],
  'g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('p').map_(Anon.bothE().label().groupCount()).as_('r').select('p', 'r'),
  ],
  'g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V().choose(Anon.outE().count().is_(xx1), Anon.as_('a'), Anon.as_('b')).choose(Anon.select('a'), Anon.select('a'), Anon.select('b')),
  ],
  'g_VX1X_groupXaX_byXconstantXaXX_byXnameX_selectXaX_selectXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).group('a').by(Anon.constant('a')).by(Anon.values('name')).barrier().select('a').select('a'),
  ],
  'g_VX1X_asXhereX_out_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('here').out().select('here'),
  ],
  'g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).as_('here').out().select('here'),
  ],
  'g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).out().as_('here').has('lang', 'java').select('here').values('name'),
  ],
  'g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().as_('here').inV().has('name', 'vadas').select('here'),
  ],
  'g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').has('weight', GDouble(1.0)).as_('here').inV().has('name', 'josh').select('here'),
  ],
  'g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').as_('here').has('weight', GDouble(1.0)).as_('fake').inV().has('name', 'josh').select('here'),
  ],
  'g_V_asXhereXout_name_selectXhereX': <Function>[
    (GraphTraversalSource g) => g.V().as_('here').out().values('name').select('here'),
  ],
  'g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').union(Anon.as_('project').in_('created').has('name', 'marko').select('project'), Anon.as_('project').in_('created').in_('knows').has('name', 'marko').select('project')).groupCount().by('name'),
  ],
  'g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX': <Function>[
    (GraphTraversalSource g) => g.V().until(Anon.out().out()).repeat(Anon.in_().as_('a')).select('a').by(Anon.tail(scope.local).values('name')),
  ],
  'g_V_outE_weight_groupCount_selectXkeysX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').groupCount().select(column.keys).unfold(),
  ],
  'g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').as_('name').as_('language').as_('creators').select('name', 'language', 'creators').by('name').by('lang').by(Anon.in_('created').values('name').fold().order(scope.local)),
  ],
  'g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').groupCount().unfold().select(column.keys).unfold(),
  ],
  'g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').groupCount().unfold().select(column.values).unfold(),
  ],
  'g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().until(Anon.out().out()).repeat(Anon.in_().as_('a').in_().as_('b')).select('a', 'b').by('name'),
  ],
  'g_V_outE_weight_groupCount_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').groupCount().select(column.values).unfold(),
  ],
  'g_V_asXaX_whereXoutXknowsXX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').where(Anon.out('knows')).select('a'),
  ],
  'g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXfirst_aX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').repeat(Anon.out().as_('a')).times(GInt(2)).select(pop.first, 'a'),
  ],
  'g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('knows').as_('b').local(Anon.select('a', 'b').by('name')),
  ],
  'g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).as_('a').repeat(Anon.out().as_('a')).times(GInt(2)).select(pop.last, 'a'),
  ],
  'g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').as_('here').has('weight', GDouble(1.0)).inV().has('name', 'josh').select('here'),
  ],
  'g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').has('name', 'marko').as_('b').as_('c').select('a', 'b', 'c').by().by('name').by('age'),
  ],
  'g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().outE().values('weight').groupCount().select(column.values).unfold().groupCount().select(column.values).unfold(),
  ],
  'g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').group('m').by().by(Anon.bothE().count()).barrier().select('m').select(Anon.select('a')),
  ],
  'g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX_byXmathX_plus_XX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').group('m').by().by(Anon.bothE().count()).barrier().select('m').select(Anon.select('a')).by(Anon.math_('_+_')),
  ],
  'g_V_asXaX_outXknowsX_asXaX_selectXall_constantXaXX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out('knows').as_('a').select(pop.all, Anon.constant('a')),
  ],
  'g_V_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().select('a'),
  ],
  'g_V_selectXaX_count': <Function>[
    (GraphTraversalSource g) => g.V().select('a').count(),
  ],
  'g_V_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().select('a', 'b'),
  ],
  'g_V_valueMap_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select('a'),
  ],
  'g_V_valueMap_selectXa_bX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select('a', 'b'),
  ],
  'g_V_selectXfirst_aX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.first, 'a'),
  ],
  'g_V_selectXfirst_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.first, 'a', 'b'),
  ],
  'g_V_valueMap_selectXfirst_aX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.first, 'a'),
  ],
  'g_V_valueMap_selectXfirst_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.first, 'a', 'b'),
  ],
  'g_V_selectXlast_aX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.last, 'a'),
  ],
  'g_V_selectXlast_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.last, 'a', 'b'),
  ],
  'g_V_valueMap_selectXlast_aX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.last, 'a'),
  ],
  'g_V_valueMap_selectXlast_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.last, 'a', 'b'),
  ],
  'g_V_selectXall_aX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.all, 'a'),
  ],
  'g_V_selectXall_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().select(pop.all, 'a', 'b'),
  ],
  'g_V_valueMap_selectXall_aX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.all, 'a'),
  ],
  'g_V_valueMap_selectXall_a_bX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().select(pop.all, 'a', 'b'),
  ],
  'g_V_asXa_bX_out_asXcX_path_selectXkeysX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a', 'b').out().as_('c').path().select(column.keys),
    (GraphTraversalSource g) => g.V().as_('a', 'b').out().as_('c').path().select(column.keys),
  ],
  'g_V_hasXperson_name_markoX_barrier_asXaX_outXknows_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').barrier().as_('a').out('knows').select('a'),
  ],
  'g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').elementMap('name').as_('a').union(Anon.identity(), Anon.identity()).select('a').select('name'),
  ],
  'g_V_hasXperson_name_markoX_count_asXaX_unionXidentity_identityX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').count().as_('a').union(Anon.identity(), Anon.identity()).select('a'),
  ],
  'g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').path().as_('a').union(Anon.identity(), Anon.identity()).select('a').unfold(),
  ],
  'g_EX11X_propertiesXweightX_asXaX_selectXaX_byXkeyX': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11).properties('weight').as_('a').select('a').by(t.key_),
  ],
  'g_EX11X_propertiesXweightX_asXaX_selectXaX_byXvalueX': <Function>[
    (GraphTraversalSource g, {dynamic eid11}) => g.E(eid11).properties('weight').as_('a').select('a').by(t.value_),
  ],
  'g_V_asXaX_selectXaX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').select('a').by('age'),
  ],
  'g_V_asXa_nX_selectXa_nX_byXageX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a', 'n').select('a', 'n').by('age').by('name'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_asXaX_selectXaX_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().as_('a').select('a').by('age'),
  ],
  'g_withSideEffectXk_nullX_injectXxX_selectXkX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('k', null).inject('x').select('k'),
  ],
  'g_V_out_in_selectXall_a_a_aX_byXunfold_name_foldX': <Function>[
    (GraphTraversalSource g) => g.addV('A').property('name', 'a1').as_('a1').addV('B').property('name', 'b1').as_('b1').addE('ab').from_('a1').to('b1'),
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').in_().as_('a').select(pop.all, 'a', 'a', 'a').by(Anon.unfold().values('name').fold()),
  ],
  'g_withoutStrategiesXLazyBarrierStrategyX_V_asXlabelX_localXaggregate_xX_selectXxX_selectXlabelX': <Function>[
    (GraphTraversalSource g) => g.withoutStrategies(LazyBarrierStrategy).V().as_('label').local(Anon.aggregate('x')).select('x').select('label'),
  ],
  'g_V_name_asXaX_selectXfirst_aX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').as_('a').select(pop.first, 'a'),
  ],
  'g_V_name_asXaX_selectXlast_aX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').as_('a').select(pop.last, 'a'),
  ],
  'g_V_name_asXaX_selectXmixed_aX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').as_('a').select(pop.mixed, 'a'),
  ],
  'g_V_name_asXaX_selectXall_aX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').as_('a').select(pop.all, 'a'),
  ],
  'g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').concat('X').as_('a').length().as_('a').select('a'),
  ],
  'g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXfirst_aX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').concat('X').as_('a').length().as_('a').select(pop.first, 'a'),
  ],
  'g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXlast_aX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').concat('X').as_('a').length().as_('a').select(pop.last, 'a'),
  ],
  'g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_concatXYZX_asXaX_selectXmixed_aX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').concat('X').as_('a').concat('YZ').as_('a').select(pop.mixed, 'a'),
  ],
  'g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_concatXYZX_asXaX_selectXall_aX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').as_('a').concat('X').as_('a').concat('YZ').as_('a').select(pop.all, 'a'),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.mixed, 'a').by(Anon.unfold().values('name').fold()),
  ],
  'g_V_asXaX_out_asXaX_out_asXaX_selectXall_aX_byXunfold_valuesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().as_('a').out().as_('a').out().as_('a').select(pop.all, 'a').by(Anon.unfold().values('name').fold()),
  ],
  'g_V_shortestPath': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath(),
  ],
  'g_V_both_dedup_shortestPath': <Function>[
    (GraphTraversalSource g) => g.V().both().dedup().shortestPath(),
  ],
  'g_V_shortestPath_edgesIncluded': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.includeEdges'),
  ],
  'g_V_shortestPath_directionXINX': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.edges', direction.in_),
  ],
  'g_V_shortestPath_edgesXoutEX': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.edges', Anon.outE()),
  ],
  'g_V_shortestPath_edgesIncluded_edgesXoutEX': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.includeEdges').with_('~tinkerpop.shortestPath.edges', Anon.outE()),
  ],
  'g_V_hasXname_markoX_shortestPath': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').shortestPath(),
  ],
  'g_V_shortestPath_targetXhasXname_markoXX': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.target', Anon.has('name', 'marko')),
  ],
  'g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX': <Function>[
    (GraphTraversalSource g) => g.V().shortestPath().with_('~tinkerpop.shortestPath.target', Anon.values('name').is_('marko')),
  ],
  'g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').shortestPath().with_('~tinkerpop.shortestPath.target', Anon.hasLabel('software')),
  ],
  'g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').shortestPath().with_('~tinkerpop.shortestPath.target', Anon.has('name', 'josh')).with_('~tinkerpop.shortestPath.distance', 'weight'),
  ],
  'g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'daniel').shortestPath().with_('~tinkerpop.shortestPath.target', Anon.has('name', 'stephen')).with_('~tinkerpop.shortestPath.edges', Anon.bothE('uses')),
  ],
  'g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX': <Function>[
    (GraphTraversalSource g) => g.V().has('song', 'name', 'MIGHT AS WELL').shortestPath().with_('~tinkerpop.shortestPath.target', Anon.has('song', 'name', 'MAYBE YOU KNOW HOW I FEEL')).with_('~tinkerpop.shortestPath.edges', Anon.outE('followedBy')).with_('~tinkerpop.shortestPath.distance', 'weight'),
  ],
  'g_V_hasXname_markoX_shortestPath_maxDistanceX1X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').shortestPath().with_('~tinkerpop.shortestPath.maxDistance', GInt(1)),
  ],
  'g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'vadas').shortestPath().with_('~tinkerpop.shortestPath.distance', 'weight').with_('~tinkerpop.shortestPath.maxDistance', GDouble(1.3)),
  ],
  'g_injectXthat_this_testX_spiltXhX': <Function>[
    (GraphTraversalSource g) => g.inject('that', 'this', 'test', null).split('h'),
  ],
  'g_injectXhello_worldX_spiltXnullX': <Function>[
    (GraphTraversalSource g) => g.inject('hello world').split(null),
  ],
  'g_injectXthat_this_test_nullX_splitXemptyX': <Function>[
    (GraphTraversalSource g) => g.inject('that', 'this', 'test', null).split(''),
  ],
  'g_injectXListXa_bXcX_splitXa_bX': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).split('a'),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_splitXnullX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').split(null),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_order_fold_splitXlocal_aX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').order().fold().split(scope.local, 'a').unfold(),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_order_fold_splitXlocal_emptyX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').order().fold().split(scope.local, '').unfold(),
  ],
  'g_injectXthat_this_testX_substringX1_8X': <Function>[
    (GraphTraversalSource g) => g.inject('test', 'hello world', null).substring(GInt(1), GInt(8)),
  ],
  'g_injectXListXa_bXcX_substringX1_2X': <Function>[
    (GraphTraversalSource g) => g.inject(['aa', 'bb']).substring(GInt(1), GInt(2)),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_substringX2X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(2)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_substringX1_4X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(1), GInt(4)),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_order_fold_substringXlocal_2X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').order().fold().substring(scope.local, GInt(2)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_order_fold_substringXlocal_1_4X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').order().fold().substring(scope.local, GInt(1), GInt(4)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_substringX1_0X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(1), GInt(0)),
  ],
  'g_V_hasLabelXpersonX_valueXnameX_substringXneg3X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').values('name').substring(GInt(-3)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_substringX1_neg1X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(1), GInt(-1)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_substringXneg4_2X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(-4), GInt(2)),
  ],
  'g_V_hasLabelXsoftwareX_valueXnameX_substringXneg3_neg1X': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').values('name').substring(GInt(-3), GInt(-1)),
  ],
  'g_V_injectX127b_1bX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(127), GByte(1)).sum(),
  ],
  'g_V_injectX_128b__1bX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(-128), GByte(-1)).sum(),
  ],
  'g_V_injectX32767s_1sX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GShort(32767), GShort(1)).sum(),
  ],
  'g_V_injectX_32768s__1sX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GShort(-32768), GShort(-1)).sum(),
  ],
  'g_V_injectX2147483647i_1iX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(2147483647), GInt(1)).sum(),
  ],
  'g_V_injectX_2147483648i__1iX_sumXX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(-2147483648), GInt(-1)).sum(),
  ],
  'g_V_age_sum': <Function>[
    (GraphTraversalSource g) => g.V().values('age').sum(),
  ],
  'g_V_foo_sum': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').sum(),
  ],
  'g_V_age_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').fold().sum(scope.local),
  ],
  'g_V_foo_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('foo').fold().sum(scope.local),
  ],
  'g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').group().by('name').by(Anon.bothE().values('weight').sum()),
  ],
  'g_V_aggregateXaX_byXageX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').sum(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').sum(scope.local),
  ],
  'g_V_aggregateXaX_byXageX_capXaX_unfold_sum': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('age').cap('a').unfold().sum(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_sum': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('age').cap('a').unfold().sum(),
  ],
  'g_V_aggregateXaX_byXfooX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').sum(scope.local),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').sum(scope.local),
  ],
  'g_V_aggregateXaX_byXfooX_capXaX_unfold_sum': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').by('foo').cap('a').unfold().sum(),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_sum': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('a').by('foo').cap('a').unfold().sum(),
  ],
  'g_injectXnull_10_5_nullX_sum': <Function>[
    (GraphTraversalSource g) => g.inject(null, GInt(10), GInt(5), null).sum(),
  ],
  'g_injectXlistXnull_10_5_nullXX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([null, GInt(10), GInt(5), null]).sum(scope.local),
  ],
  'g_VX1X_valuesXageX_sumXlocalX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).values('age').sum(scope.local),
  ],
  'g_V_localXunionXvaluesXageX_outE_valuesXweightXX_foldX_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.union(Anon.values('age'), Anon.outE().values('weight')).fold()).sum(scope.local),
  ],
  'g_V_age_injectX1000nX_sum': <Function>[
    (GraphTraversalSource g) => g.V().values('age').inject(BigInt.parse('1000')).sum(),
  ],
  'g_injectX1b_2b_3bX_sum': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GByte(3)).sum(),
  ],
  'g_injectX1b_2b_3sX_sum': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GShort(3)).sum(),
  ],
  'g_injectX1b_26b_3iX_sum': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GInt(3)).sum(),
  ],
  'g_injectX1f_26f_3fX_sum': <Function>[
    (GraphTraversalSource g) => g.inject(GFloat(1), GFloat(2), GFloat(3)).sum(),
  ],
  'g_V_age_injectX1000nX_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('age').inject(BigInt.parse('1000')).fold().sum(scope.local),
  ],
  'g_injectX1b_2b_3bX_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GByte(3)).fold().sum(scope.local),
  ],
  'g_injectX1b_2b_3sX_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GShort(3)).fold().sum(scope.local),
  ],
  'g_injectX1b_26b_3iX_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(GByte(1), GByte(2), GInt(3)).fold().sum(scope.local),
  ],
  'g_injectX1f_26f_3fX_fold_sumXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(GFloat(1), GFloat(2), GFloat(3)).fold().sum(scope.local),
  ],
  'g_injectXfeature_test_nullX_toLower': <Function>[
    (GraphTraversalSource g) => g.inject('FEATURE', 'tESt', null).toLower(),
  ],
  'g_injectXfeature_test_nullX_toLowerXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(['FEATURE', 'tESt', null]).toLower(scope.local),
  ],
  'g_injectXListXa_bXX_toLower': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).toLower(),
  ],
  'g_V_valuesXnameX_toLower': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'MARKO').property('age', GInt(29)).as_('marko').addV('person').property('name', 'VADAS').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'LOP').property('lang', 'java').as_('lop').addV('person').property('name', 'JOSH').property('age', GInt(32)).as_('josh').addV('software').property('name', 'RIPPLE').property('lang', 'java').as_('ripple').addV('person').property('name', 'PETER').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').toLower(),
  ],
  'g_V_valuesXnameX_toLowerXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'MARKO').property('age', GInt(29)).as_('marko').addV('person').property('name', 'VADAS').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'LOP').property('lang', 'java').as_('lop').addV('person').property('name', 'JOSH').property('age', GInt(32)).as_('josh').addV('software').property('name', 'RIPPLE').property('lang', 'java').as_('ripple').addV('person').property('name', 'PETER').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').toLower(scope.local),
  ],
  'g_V_valuesXnameX_order_fold_toLowerXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'MARKO').property('age', GInt(29)).as_('marko').addV('person').property('name', 'VADAS').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'LOP').property('lang', 'java').as_('lop').addV('person').property('name', 'JOSH').property('age', GInt(32)).as_('josh').addV('software').property('name', 'RIPPLE').property('lang', 'java').as_('ripple').addV('person').property('name', 'PETER').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').order().fold().toLower(scope.local),
  ],
  'g_injectXfeature_test_nullX_toUpper': <Function>[
    (GraphTraversalSource g) => g.inject('feature', 'tESt', null).toUpper(),
  ],
  'g_injectXfeature_test_nullX_toUpperXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(['feature', 'tESt', null]).toUpper(scope.local),
  ],
  'g_injectXListXa_bXX_toUpper': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).toUpper(),
  ],
  'g_V_valuesXnameX_toUpper': <Function>[
    (GraphTraversalSource g) => g.V().values('name').toUpper(),
  ],
  'g_V_valuesXnameX_toUpperXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').toUpper(scope.local),
  ],
  'g_V_valuesXnameX_order_fold_toUpperXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').order().fold().toUpper(scope.local),
  ],
  'g_injectX__feature___test__nullX_trim': <Function>[
    (GraphTraversalSource g) => g.inject('  feature  ', ' one test ', null, '', ' ', '　abc', 'abc　', '　abc　', '　　').trim(),
  ],
  'g_injectX__feature___test__nullX_trimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject(['  feature  ', ' one test ', null, '', ' ', '　abc', 'abc　', '　abc　', '　　']).trim(scope.local),
  ],
  'g_injectXListXa_bXX_trim': <Function>[
    (GraphTraversalSource g) => g.inject(['a', 'b']).trim(),
  ],
  'g_injectXListX1_2XX_trimXlocalX': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(2)]).trim(scope.local),
  ],
  'g_V_valuesXnameX_trim': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').trim(),
  ],
  'g_V_valuesXnameX_order_fold_trimXlocalX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', ' marko ').property('age', GInt(29)).as_('marko').addV('person').property('name', '  vadas  ').property('age', GInt(27)).as_('vadas').addV('software').property('name', '  lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh  ').property('age', GInt(32)).as_('josh').addV('software').property('name', '   ripple   ').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g) => g.V().values('name').order().fold().trim(scope.local),
  ],
  'g_V_localXoutE_foldX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.outE().fold()).unfold(),
  ],
  'g_V_valueMap_unfold_mapXselectXkeysXX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().unfold().map_(Anon.select(column.keys)),
  ],
  'g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold': <Function>[
    (GraphTraversalSource g, {dynamic vid6, dynamic vid1}) => g.V(vid1).repeat(Anon.both().simplePath()).until(Anon.hasId(vid6)).path().by('name').unfold(),
  ],
  'g_V_valueMap': <Function>[
    (GraphTraversalSource g) => g.V().valueMap(),
  ],
  'g_V_valueMapXtrueX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap(true),
  ],
  'g_V_valueMap_withXtokensX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap().with_(WithOptions.tokens),
  ],
  'g_V_valueMapXname_ageX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age'),
  ],
  'g_V_valueMapXtrue_name_ageX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap(true, 'name', 'age'),
  ],
  'g_V_valueMapXname_ageX_withXtokensX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age').with_(WithOptions.tokens),
  ],
  'g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age').with_(WithOptions.tokens, WithOptions.labels).by(Anon.unfold()),
  ],
  'g_V_valueMapXname_ageX_withXtokens_idsX_byXunfoldX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age').with_(WithOptions.tokens, WithOptions.ids).by(Anon.unfold()),
  ],
  'g_VX1X_outXcreatedX_valueMap': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('created').valueMap(),
  ],
  'g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMapXtrueX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').filter_(Anon.outE('created')).valueMap(true),
  ],
  'g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').filter_(Anon.outE('created')).valueMap().with_(WithOptions.tokens),
  ],
  'g_VX1X_valueMapXname_locationX_byXunfoldX_by': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).valueMap('name', 'location').by(Anon.unfold()).by(),
  ],
  'g_V_valueMapXname_age_nullX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age', null),
  ],
  'g_V_valueMapXname_ageX_byXisXxXXbyXunfoldX': <Function>[
    (GraphTraversalSource g) => g.V().valueMap('name', 'age').by(Anon.is_('x')).by(Anon.unfold()),
  ],
  'g_VXnullX': <Function>[
    (GraphTraversalSource g) => g.V(null),
  ],
  'g_VXlistXnullXX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1),
  ],
  'g_VX1_nullX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1, null),
  ],
  'g_VXlistX1_2_3XX_name': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1).values('name'),
  ],
  'g_VXlistXv1_v2_v3XX_name': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1).values('name'),
  ],
  'g_V': <Function>[
    (GraphTraversalSource g) => g.V(),
  ],
  'g_VXv1X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out(),
  ],
  'g_VX1X_out': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out(),
  ],
  'g_VX2X_in': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).in_(),
  ],
  'g_VX4X_both': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).both(),
  ],
  'g_VX1X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE(),
  ],
  'g_VX2X_outE': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).inE(),
  ],
  'g_VX4X_bothEXcreatedX': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).bothE('created'),
  ],
  'g_VX4X_bothEXcreatedvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid4}) => g.V(vid4).bothE(xx1),
  ],
  'g_VX4X_bothE': <Function>[
    (GraphTraversalSource g, {dynamic vid4}) => g.V(vid4).bothE(),
  ],
  'g_V_out_outE_inV_inE_inV_both_name': <Function>[
    (GraphTraversalSource g) => g.V().out().outE().inV().inE().inV().both().values('name'),
  ],
  'g_VX2X_inE': <Function>[
    (GraphTraversalSource g, {dynamic vid2}) => g.V(vid2).bothE(),
  ],
  'g_VX1X_outXknowsX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('knows'),
  ],
  'g_VX1AsStringX_outXknowsX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('knows'),
  ],
  'g_VX1X_outXknows_createdX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out('knows', 'created'),
  ],
  'g_VX1X_outXknowsvar_createdvarX': <Function>[
    (GraphTraversalSource g, {dynamic xx3, dynamic xx2, dynamic vid1}) => g.V(vid1).out(xx2, xx3),
  ],
  'g_V_out_out': <Function>[
    (GraphTraversalSource g) => g.V().out().out(),
  ],
  'g_VX1X_out_out_out': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().out(),
  ],
  'g_VX1X_out_name': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().values('name'),
  ],
  'g_VX1X_to_XOUT_knowsX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).to(direction.OUT, 'knows'),
  ],
  'g_VX1_2_3_4X_name': <Function>[
    (GraphTraversalSource g, {dynamic vid4, dynamic vid3, dynamic vid2, dynamic vid1}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic vid4, dynamic vid3, dynamic vid2, dynamic vid1}) => g.V().has('software', 'name', 'lop').drop(),
    (GraphTraversalSource g, {dynamic vid4, dynamic vid3, dynamic vid2, dynamic vid1}) => g.V(vid1, vid2, vid3, vid4),
  ],
  'g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').V().hasLabel('software').values('name'),
  ],
  'g_V_hasLabelXloopsX_bothEXselfX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('loops').bothE('self'),
  ],
  'g_V_hasLabelXloopsX_bothXselfX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('loops').both('self'),
  ],
  'g_injectX1X_VXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).V(null),
  ],
  'g_injectX1X_VX1_nullX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.inject(GInt(1)).V(vid1, null),
  ],
  'g_VX1X_V_valuesXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).V().values('name'),
  ],
  'g_V_outXknowsX_V_name': <Function>[
    (GraphTraversalSource g) => g.V().out('knows').V().values('name'),
  ],
  'g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name': <Function>[
    (GraphTraversalSource g) => g.V().has('artist', 'name', 'Garcia').in_('sungBy').as_('song').V().has('artist', 'name', 'Willie_Dixon').in_('writtenBy').where(P.eq('song')).values('name'),
  ],
  'g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX': <Function>[
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.addV('person').property('name', 'marko').property('age', GInt(29)).as_('marko').addV('person').property('name', 'vadas').property('age', GInt(27)).as_('vadas').addV('software').property('name', 'lop').property('lang', 'java').as_('lop').addV('person').property('name', 'josh').property('age', GInt(32)).as_('josh').addV('software').property('name', 'ripple').property('lang', 'java').as_('ripple').addV('person').property('name', 'peter').property('age', GInt(35)).as_('peter').addE('knows').from_('marko').to('vadas').property('weight', GDouble(0.5)).addE('knows').from_('marko').to('josh').property('weight', GDouble(1.0)).addE('created').from_('marko').to('lop').property('weight', GDouble(0.4)).addE('created').from_('josh').to('ripple').property('weight', GDouble(1.0)).addE('created').from_('josh').to('lop').property('weight', GDouble(0.4)).addE('created').from_('peter').to('lop').property('weight', GDouble(0.2)),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V().hasLabel('person').as_('p').V(xx1).addE('uses').from_('p'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.E().hasLabel('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid1).outE('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid2).outE('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid3).inE('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid4).outE('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid5).inE('uses'),
    (GraphTraversalSource g, {dynamic xx1, dynamic vid1, dynamic vid2, dynamic vid3, dynamic vid4, dynamic vid5, dynamic vid6}) => g.V(vid6).outE('uses'),
  ],
  'InjectXnullX_eqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.eq(null)),
  ],
  'InjectXnullX_neqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.neq(null)),
  ],
  'InjectXnullX_ltXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lt(null)),
  ],
  'InjectXnullX_lteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lte(null)),
  ],
  'InjectXnullX_gtXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gt(null)),
  ],
  'InjectXnullX_gteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gte(null)),
  ],
  'InjectXNaNX_eqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.eq(double.nan)),
  ],
  'InjectXNaNX_neqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.neq(double.nan)),
  ],
  'InjectXNaNX_ltXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lt(double.nan)),
  ],
  'InjectXNaNX_lteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lte(double.nan)),
  ],
  'InjectXNaNX_gtXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gt(double.nan)),
  ],
  'InjectXNaNX_gteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gte(double.nan)),
  ],
  'InjectX1dX_eqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.eq(double.nan)),
  ],
  'InjectX1dX_neqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.neq(double.nan)),
  ],
  'InjectX1dX_ltXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lt(double.nan)),
  ],
  'InjectX1dX_lteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lte(double.nan)),
  ],
  'InjectX1dX_gtXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gt(double.nan)),
  ],
  'InjectX1dX_gteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gte(double.nan)),
  ],
  'InjectXNaNX_eqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.eq(GDouble(1.0))),
  ],
  'InjectXNaNX_neqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.neq(GDouble(1.0))),
  ],
  'InjectXNaNX_ltX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lt(GDouble(1.0))),
  ],
  'InjectXNaNX_lteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lte(GDouble(1.0))),
  ],
  'InjectXNaNX_gtX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gt(GDouble(1.0))),
  ],
  'InjectXNaNX_gteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gte(GDouble(1.0))),
  ],
  'InjectX1dX_eqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.eq(null)),
  ],
  'InjectX1dX_neqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.neq(null)),
  ],
  'InjectX1dX_ltXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lt(null)),
  ],
  'InjectX1dX_lteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lte(null)),
  ],
  'InjectX1dX_gtXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gt(null)),
  ],
  'InjectX1dX_gteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gte(null)),
  ],
  'InjectXnullX_eqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.eq(GDouble(1.0))),
  ],
  'InjectXnullX_neqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.neq(GDouble(1.0))),
  ],
  'InjectXnullX_ltX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lt(GDouble(1.0))),
  ],
  'InjectXnullX_lteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lte(GDouble(1.0))),
  ],
  'InjectXnullX_gtX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gt(GDouble(1.0))),
  ],
  'InjectXnullX_gteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gte(GDouble(1.0))),
  ],
  'InjectXnullX_eqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.eq(double.nan)),
  ],
  'InjectXnullX_neqXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.neq(double.nan)),
  ],
  'InjectXnullX_ltXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lt(double.nan)),
  ],
  'InjectXnullX_lteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.lte(double.nan)),
  ],
  'InjectXnullX_gtXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gt(double.nan)),
  ],
  'InjectXnullX_gteXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(null).is_(P.gte(double.nan)),
  ],
  'InjectXNaNX_eqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.eq(null)),
  ],
  'InjectXNaNX_neqXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.neq(null)),
  ],
  'InjectXNaNX_ltXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lt(null)),
  ],
  'InjectXNaNX_lteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.lte(null)),
  ],
  'InjectXNaNX_gtXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gt(null)),
  ],
  'InjectXNaNX_gteXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(double.nan).is_(P.gte(null)),
  ],
  'InjectXfooX_eqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.eq(GDouble(1.0))),
  ],
  'InjectXfooX_neqX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.neq(GDouble(1.0))),
  ],
  'InjectXfooX_ltX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.lt(GDouble(1.0))),
  ],
  'InjectXfooX_lteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.lte(GDouble(1.0))),
  ],
  'InjectXfooX_gtX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.gt(GDouble(1.0))),
  ],
  'InjectXfooX_gteX1dX': <Function>[
    (GraphTraversalSource g) => g.inject('foo').is_(P.gte(GDouble(1.0))),
  ],
  'InjectX1dX_eqXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.eq('foo')),
  ],
  'InjectX1dX_neqXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.neq('foo')),
  ],
  'InjectX1dX_ltXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lt('foo')),
  ],
  'InjectX1dX_lteXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.lte('foo')),
  ],
  'InjectX1dX_gtXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gt('foo')),
  ],
  'InjectX1dX_gteXfooX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1.0)).is_(P.gte('foo')),
  ],
  'InjectX1dX_andXtrue_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXtrue_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).and_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_andXtrue_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_isXtrue_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).and_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_andXtrue_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.lt(double.nan))),
  ],
  'InjectX1dX_isXtrue_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).and_(P.lt(double.nan))),
  ],
  'InjectX1dX_andXfalse_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXfalse_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).and_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_andXfalse_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_isXfalse_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).and_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_andXfalse_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.lt(double.nan))),
  ],
  'InjectX1dX_isXfalse_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).and_(P.lt(double.nan))),
  ],
  'InjectX1dX_andXerror_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXerror_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).and_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_andXerror_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(GInt(2)))),
  ],
  'InjectX1dX_isXerror_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).and_(P.gt(GInt(2)))),
  ],
  'InjectX1dX_andXerror_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).and_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(double.nan))),
  ],
  'InjectX1dX_isXerror_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).and_(P.gt(double.nan))),
  ],
  'InjectX1dX_orXtrue_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXtrue_or_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).or_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_orXtrue_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_isXtrue_or_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).or_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_orXtrue_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.eq(GInt(1))), Anon.is_(P.lt(double.nan))),
  ],
  'InjectX1dX_isXtrue_or_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.eq(GInt(1)).or_(P.lt(double.nan))),
  ],
  'InjectX1dX_orXfalse_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXfalse_or_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).or_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_orXfalse_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_isXfalse_or_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).or_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_orXfalse_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.neq(GInt(1))), Anon.is_(P.lt(double.nan))),
  ],
  'InjectX1dX_isXfalse_or_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.neq(GInt(1)).or_(P.lt(double.nan))),
  ],
  'InjectX1dX_orXerror_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_isXerror_or_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).or_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_orXerror_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(GInt(2)))),
  ],
  'InjectX1dX_isXerror_or_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).or_(P.gt(GInt(2)))),
  ],
  'InjectX1dX_orXerror_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).or_(Anon.is_(P.lt(double.nan)), Anon.is_(P.gt(double.nan))),
  ],
  'InjectX1dX_isXerror_or_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).is_(P.lt(double.nan).or_(P.gt(double.nan))),
  ],
  'InjectX1dX_notXtrueX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).not_(Anon.is_(P.gt(GInt(0)))),
  ],
  'InjectX1dX_notXfalseX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).not_(Anon.is_(P.lt(GInt(0)))),
  ],
  'InjectX1dX_notXNaNX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).not_(Anon.is_(P.gt(double.nan))),
  ],
  'InjectX1dX_notXisXeqXNaNXXX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).not_(Anon.is_(P.eq(double.nan))),
  ],
  'InjectX1dX_notXnotXisXeqXNaNXXXX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).not_(Anon.not_(Anon.is_(P.eq(double.nan)))),
  ],
  'InjectX1dX_whereXnotXisXltXNaNXXXX': <Function>[
    (GraphTraversalSource g) => g.inject(GDouble(1)).where(Anon.inject(GInt(1)).not_(Anon.is_(P.lt(double.nan)))),
  ],
  'InjectX1dX_xorXtrue_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.eq(GInt(1))))), Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.eq(GInt(1))))))),
  ],
  'InjectX1dX_xorXtrue_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.gt(GInt(1))))), Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.eq(GInt(1))))))),
  ],
  'InjectX1dX_xorXtrue_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.lt(double.nan)))), Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.eq(GInt(1))))))),
  ],
  'InjectX1dX_xorXfalse_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.eq(GInt(1))))), Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.gt(GInt(1))))))),
  ],
  'InjectX1dX_xorXfalse_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.gt(GInt(1))))), Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.gt(GInt(1))))))),
  ],
  'InjectX1dX_xorXfalse_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.lt(double.nan)))), Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.gt(GInt(1))))))),
  ],
  'InjectX1dX_xorXerror_trueX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.eq(GInt(1))))), Anon.and_(Anon.is_(P.eq(GInt(1))), Anon.not_(Anon.is_(P.lt(double.nan)))))),
  ],
  'InjectX1dX_xorXerror_falseX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.gt(GInt(1))))), Anon.and_(Anon.is_(P.gt(GInt(1))), Anon.not_(Anon.is_(P.lt(double.nan)))))),
  ],
  'InjectX1dX_xorXerror_errorX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1)).filter_(Anon.or_(Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.lt(double.nan)))), Anon.and_(Anon.is_(P.lt(double.nan)), Anon.not_(Anon.is_(P.lt(double.nan)))))),
  ],
  'InjectXInfX_eqXInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.infinity).is_(P.eq(double.infinity)),
  ],
  'InjectXInfX_neqXInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.infinity).is_(P.neq(double.infinity)),
  ],
  'InjectXNegInfX_eqXNegInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.negativeInfinity).is_(P.eq(double.negativeInfinity)),
  ],
  'InjectXNegInfX_neqXNegInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.negativeInfinity).is_(P.neq(double.negativeInfinity)),
  ],
  'InjectXInfX_gtXNegInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.infinity).is_(P.gt(double.negativeInfinity)),
  ],
  'InjectXInfX_ltXNegInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.infinity).is_(P.lt(double.negativeInfinity)),
  ],
  'InjectXNegInfX_ltXInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.negativeInfinity).is_(P.lt(double.infinity)),
  ],
  'InjectXNegInfX_gtXInfX': <Function>[
    (GraphTraversalSource g) => g.inject(double.negativeInfinity).is_(P.gt(double.infinity)),
  ],
  'Primitives_Number_eqXbyteX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXshortX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXintX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXlongX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXbigintX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXfloatX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXdoubleX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'Primitives_Number_eqXbigdecimalX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.inject([GByte(1), GShort(1), GInt(1), GLong(1), GFloat(1), GDouble(1), GInt(1000), GDouble(1), BigInt.parse('1')]).unfold().where(Anon.is_(xx1)),
  ],
  'g_V_values_order': <Function>[
    (GraphTraversalSource g) => g.V().values().order(),
  ],
  'g_V_properties_order': <Function>[
    (GraphTraversalSource g) => g.V().properties().order(),
  ],
  'g_V_properties_order_id': <Function>[
    (GraphTraversalSource g) => g.V().properties().order().id(),
  ],
  'g_E_properties_order_value': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addE('self').from_('a').to('a').property('weight', GDouble(0.5)).property('a', GInt(10)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(11)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(12)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(13)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(14)).addE('self').from_('a').to('a').property('weight', GDouble(0.2)).property('a', GInt(15)),
    (GraphTraversalSource g) => g.E().properties().order().value_(),
  ],
  'g_E_properties_order_byXdescX_value': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addE('self').from_('a').to('a').property('weight', GDouble(0.5)).property('a', GInt(10)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(11)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(12)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(13)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(14)).addE('self').from_('a').to('a').property('weight', GDouble(0.2)).property('a', GInt(15)),
    (GraphTraversalSource g) => g.E().properties().order().by(order.desc).value_(),
  ],
  'g_E_properties_order': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addE('self').from_('a').to('a').property('weight', GDouble(0.5)).property('a', GInt(10)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(11)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(12)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(13)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(14)).addE('self').from_('a').to('a').property('weight', GDouble(0.2)).property('a', GInt(15)),
    (GraphTraversalSource g) => g.E().properties().order(),
  ],
  'g_E_properties_order_byXdescX': <Function>[
    (GraphTraversalSource g) => g.addV('person').property('name', 'alice').as_('a').addE('self').from_('a').to('a').property('weight', GDouble(0.5)).property('a', GInt(10)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(11)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(12)).addE('self').from_('a').to('a').property('weight', GDouble(1.0)).property('a', GInt(13)).addE('self').from_('a').to('a').property('weight', GDouble(0.4)).property('a', GInt(14)).addE('self').from_('a').to('a').property('weight', GDouble(0.2)).property('a', GInt(15)),
    (GraphTraversalSource g) => g.E().properties().order().by(order.desc),
  ],
  'g_inject_order': <Function>[
    (GraphTraversalSource g) => g.inject('zzz', 'foo', UuidValue('6100808b-62f9-42b7-957e-ed66c30f40d1'), ['a', 'b', 'c', 'd'], GInt(1), DateTime.parse('2023-08-01T00:00Z'), ['a', 'b', 'c'], {'a': 'a', 'b': 'b'}, null, GDouble(2.0), DateTime.parse('2023-01-01T00:00Z'), <dynamic>{'x', 'y', 'z'}, {'a': 'a', 'b': false, 'c': 'c'}, 'bar', UuidValue('5100808b-62f9-42b7-957e-ed66c30f40d1'), true, false, double.infinity, double.nan, double.negativeInfinity).order(),
  ],
  'g_inject_order_byXdescX': <Function>[
    (GraphTraversalSource g) => g.inject('zzz', 'foo', UuidValue('6100808b-62f9-42b7-957e-ed66c30f40d1'), ['a', 'b', 'c', 'd'], GInt(1), DateTime.parse('2023-08-01T00:00Z'), ['a', 'b', 'c'], {'a': 'a', 'b': 'b'}, null, GDouble(2.0), DateTime.parse('2023-01-01T00:00Z'), <dynamic>{'x', 'y', 'z'}, {'a': 'a', 'b': false, 'c': 'c'}, 'bar', UuidValue('5100808b-62f9-42b7-957e-ed66c30f40d1'), true, false, double.infinity, double.nan, double.negativeInfinity).order().by(order.desc),
  ],
  'g_V_out_out_order_byXascX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().order().by(order.asc),
  ],
  'g_V_out_out_order_byXdescX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().order().by(order.desc),
  ],
  'g_V_out_out_asXheadX_path_order_byXascX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().as_('head').path().order().by(order.asc).select('head'),
  ],
  'g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().as_('head').path().order().by(order.desc).select('head'),
  ],
  'g_V_out_outE_order_byXascX': <Function>[
    (GraphTraversalSource g) => g.V().out().outE().order().by(order.asc),
  ],
  'g_V_out_outE_order_byXdescX': <Function>[
    (GraphTraversalSource g) => g.V().out().outE().order().by(order.desc),
  ],
  'g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().outE().as_('head').path().order().by(order.asc).select('head'),
  ],
  'g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().outE().as_('head').path().order().by(order.desc).select('head'),
  ],
  'g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value': <Function>[
    (GraphTraversalSource g) => g.V().out().out().properties().as_('head').path().order().by(order.asc).select('head').value_(),
  ],
  'g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value': <Function>[
    (GraphTraversalSource g) => g.V().out().out().properties().as_('head').path().order().by(order.desc).select('head').value_(),
  ],
  'g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().values().as_('head').path().order().by(order.asc).select('head'),
  ],
  'g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX': <Function>[
    (GraphTraversalSource g) => g.V().out().out().values().as_('head').path().order().by(order.desc).select('head'),
  ],
  'g_V_valueXnameX_aggregateXxX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().values('name').aggregate('x').cap('x'),
  ],
  'g_V_aggregateXxX_byXnameX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('x').by('name').cap('x'),
  ],
  'g_V_out_aggregateXaX_path': <Function>[
    (GraphTraversalSource g) => g.V().out().aggregate('a').path(),
  ],
  'g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').aggregate('x').by('age').cap('x').as_('y').select('y'),
  ],
  'g_V_aggregateXxX_byXageX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('x').by('age').cap('x'),
  ],
  'g_V_localXaggregateXxX_byXageXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('x').by('age')).cap('x'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_localXaggregateXxX_byXageXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().local(Anon.aggregate('x').by('age')).cap('x'),
  ],
  'g_V_localX_aggregateXa_byXnameXX_out_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a').by('name')).out().cap('a'),
  ],
  'g_VX1X_localXaggregateXaX_byXnameXX_out_localXaggregateXaX_byXnameXX_name_capXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).local(Anon.aggregate('a').by('name')).out().local(Anon.aggregate('a').by('name')).values('name').cap('a'),
  ],
  'g_withSideEffectXa_setX_V_both_name_localXaggregateX_aXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().both().values('name').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_withSideEffectXa_set_inlineX_V_both_name_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', <dynamic>{'alice'}).V().both().values('name').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_localXaggregateXaX_byXoutEXcreatedX_countXX_out_out_localXaggregateXaX_byXinEXcreatedX_weight_sumXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a').by(Anon.outE('created').count())).out().out().local(Anon.aggregate('a').by(Anon.inE('created').values('weight').sum())).cap('a'),
  ],
  'g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('x').by(Anon.values('age').is_(P.gt(GInt(29)))).cap('x'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('x').by(Anon.values('age').is_(P.gt(GInt(29)))).cap('x'),
  ],
  'g_V_aggregateXxX_byXout_order_byXnameXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('x').by(Anon.out().order().by('name')).cap('x'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_aggregateXxX_byXout_order_byXnameXX_capXxX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().aggregate('x').by(Anon.out().order().by('name')).cap('x'),
  ],
  'g_V_aggregateXaX_hasXperson_age_gteX30XXX_capXaX_unfold_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').has('person', 'age', P.gte(GInt(30))).cap('a').unfold().values('name'),
  ],
  'g_withSideEffectXa_1_sumX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.sum).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_1_sumX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.sum).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_123_minusX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(123), operator_.minus).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_123_minusX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(123), operator_.minus).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_2_multX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(2), operator_.mult).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_2_multX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(2), operator_.mult).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_876960_divX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(876960), operator_.div).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_876960_divX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(876960), operator_.div).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_1_minX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.min).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_1_minX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.min).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_100_minX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(100), operator_.min).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_100_minX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(100), operator_.min).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_1_maxX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.max).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_1_maxX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(1), operator_.max).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_100_maxX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(100), operator_.max).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_100_maxX_V_localXaggregateX_aX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', GInt(100), operator_.max).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_true_andX_V_constantXfalseX_aggregateXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', true, operator_.and_).V().constant(false).aggregate('a').cap('a'),
  ],
  'g_withSideEffectXa_true_andX_V_constantXfalseX_localXaggregateX_aXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', true, operator_.and_).V().constant(false).local(Anon.aggregate('a')).cap('a'),
  ],
  'g_withSideEffectXa_true_orX_V_constantXfalseX_aggregateXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', true, operator_.or_).V().constant(false).aggregate('a').cap('a'),
  ],
  'g_withSideEffectXa_true_orX_V_constantXfalseX_localXaggregateX_aXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', true, operator_.or_).V().constant(false).local(Anon.aggregate('a')).cap('a'),
  ],
  'g_withSideEffectXa_1_2_3_addAllX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', [GInt(1), GInt(2), GInt(3)], operator_.addAll).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_1_2_3_addAllX_V_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', [GInt(1), GInt(2), GInt(3)], operator_.addAll).V().local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_withSideEffectXa_1_2_3_assignX_V_aggregateXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', [GInt(1), GInt(2), GInt(3)], operator_.assign).V().aggregate('a').by('age').cap('a'),
  ],
  'g_withSideEffectXa_1_2_3_assignX_V_order_byXageX_localXaggregateX_aX_byXageXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', [GInt(1), GInt(2), GInt(3)], operator_.assign).V().order().by('age').local(Anon.aggregate('a').by('age')).cap('a'),
  ],
  'g_V_localXaggregateXa_nameXX_out_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a').by('name')).out().cap('a'),
  ],
  'g_withSideEffectXa_setX_V_both_name_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().both().values('name').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_localXaggregateXaXX_outE_inV_localXaggregateXaXX_capXaX_unfold_dedup': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a')).outE().inV().local(Anon.aggregate('a')).cap('a').unfold().dedup(),
  ],
  'g_V_hasLabelXpersonX_localXaggregateXaXX_outXcreatedX_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').local(Anon.aggregate('a')).out('created').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_localXaggregateXaXX_repeatXout_localXaggregateXaXXX_timesX2X_capXaX_unfold_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a')).repeat(Anon.out().local(Anon.aggregate('a'))).times(GInt(2)).cap('a').unfold().values('name').groupCount(),
  ],
  'g_V_hasXname_markoX_localXaggregateXaXX_outXknowsX_localXaggregateXaXX_outXcreatedX_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'marko').local(Anon.aggregate('a')).out('knows').local(Anon.aggregate('a')).out('created').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_hasLabelXsoftwareX_localXaggregateXaXX_inXcreatedX_localXaggregateXaXX_outXknowsX_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('software').local(Anon.aggregate('a')).in_('created').local(Anon.aggregate('a')).out('knows').local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_localXaggregateXaXX_outE_hasXweight_lgtX0_5XX_inV_localXaggregateXaXX_capXaX_unfold_path': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a')).outE().has('weight', P.gt(GDouble(0.5))).inV().local(Anon.aggregate('a')).cap('a').unfold().path(),
  ],
  'g_V_localXaggregateXaXX_bothE_sampleX1X_otherV_localXaggregateXaXX_capXaX_unfold_groupCount_byXlabelX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a')).bothE().sample(GInt(1)).otherV().local(Anon.aggregate('a')).cap('a').unfold().groupCount().by(t.label),
  ],
  'g_V_hasLabelXpersonX_localXaggregateXaXX_outE_inV_simplePath_localXaggregateXaXX_capXaX_unfold_hasLabelXsoftwareX_count': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').local(Anon.aggregate('a')).outE().inV().simplePath().local(Anon.aggregate('a')).cap('a').unfold().hasLabel('software').count(),
  ],
  'g_V_localXaggregateXaXX_unionXout_inX_localXaggregateXaXX_capXaX_unfold_dedup_valuesXnameX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.aggregate('a')).union(Anon.out(), Anon.in_()).local(Anon.aggregate('a')).cap('a').unfold().dedup().values('name'),
  ],
  'g_V_hasXname_joshX_localXaggregateXaXX_outE_hasXweight_ltX1_0XX_inV_localXaggregateXaXX_outE_inV_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('name', 'josh').local(Anon.aggregate('a')).outE().has('weight', P.lt(GDouble(1.0))).inV().local(Anon.aggregate('a')).outE().inV().local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_hasLabelXpersonX_localXaggregateXaXX_outE_order_byXweightX_limitX1X_inV_localXaggregateXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').local(Anon.aggregate('a')).outE().order().by('weight').limit(GInt(1)).inV().local(Anon.aggregate('a')).cap('a'),
  ],
  'g_V_repeatXaggregateXaXX_timesX2X_capXaX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.aggregate('a')).times(GInt(2)).cap('a').unfold(),
  ],
  'g_V_aggregateXaX_capXaX_unfold_both': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').cap('a').unfold().both(),
  ],
  'g_V_aggregateXaX_capXaX_unfold_barrier_both': <Function>[
    (GraphTraversalSource g) => g.V().aggregate('a').cap('a').unfold().barrier().both(),
  ],
  'g_V_fail': <Function>[
    (GraphTraversalSource g) => g.V().fail(),
  ],
  'g_V_failXmsgX': <Function>[
    (GraphTraversalSource g) => g.V().fail('msg'),
  ],
  'g_V_unionXout_failX': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.out(), Anon.fail()),
  ],
  'g_V_group_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().group().by('name'),
  ],
  'g_V_group_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().group().by('age'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_group_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().group().by('age'),
  ],
  'g_V_group_byXnameX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().group().by('name').by('age'),
  ],
  'g_V_group_byXnameX_by': <Function>[
    (GraphTraversalSource g) => g.V().group().by('name').by(),
  ],
  'g_V_hasXlangX_group_byXlangX_byXcountX': <Function>[
    (GraphTraversalSource g) => g.V().has('lang').group().by('lang').by(Anon.count()),
  ],
  'g_V_group_byXoutE_countX_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().order().by('name').group().by(Anon.outE().count()).by('name'),
  ],
  'g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both('followedBy')).times(GInt(2)).group().by('songType').by(Anon.count()),
  ],
  'g_V_group_byXvaluesXnameX_substringX1XX_byXconstantX1XX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(Anon.values('name').substring(GInt(0), GInt(1))).by(Anon.constant(GInt(1))),
  ],
  'g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X': <Function>[
    (GraphTraversalSource g) => g.V().out().group().by(t.label).select('person').unfold().out('created').values('name').limit(GInt(2)),
  ],
  'g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('song').group().by('name').by(Anon.properties().groupCount().by(t.label)),
  ],
  'g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX': <Function>[
    (GraphTraversalSource g) => g.V().out('followedBy').group().by('songType').by(Anon.bothE().group().by(t.label).by(Anon.values('weight').sum())),
  ],
  'g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(t.label).by(Anon.bothE().group('a').by(t.label).by(Anon.values('weight').sum()).values('weight').sum()),
  ],
  'g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('a', {'marko': ['666'], 'noone': ['blah']}).V().group('a').by('name').by(Anon.outE().label().fold()).cap('a').unfold().group().by(column.keys).by(Anon.select(column.values).order(scope.local).by(order.asc)),
  ],
  'g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('p').out('created').group().by('name').by(Anon.select('p').values('age').sum()),
  ],
  'g_V_group_byXlabelX_byXlabel_countX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(Anon.label()).by(Anon.label().count()),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_foldX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group().by().by(Anon.out().order().fold()),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_foldX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group().by().by(Anon.out().fold()),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_orderX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group().by().by(Anon.out().order()),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_countX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group().by().by(Anon.out().order().count()),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_fold_countXlocalXX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group().by().by(Anon.out().order().fold().count(scope.local)),
  ],
  'g_V_group_by_byXout_label_foldX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group().by().by(Anon.out().label().fold()).select(column.values).unfold().order(scope.local),
  ],
  'g_V_group_by_byXout_label_dedup_foldX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group().by().by(Anon.out().label().dedup().fold()).select(column.values).unfold().order(scope.local),
  ],
  'g_V_group_by_byXout_label_limitX0X_foldX_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().group().by().by(Anon.out().label().limit(GInt(0)).fold()).select(column.values).unfold(),
  ],
  'g_V_group_by_byXout_label_limitX10X_foldX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group().by().by(Anon.out().label().limit(GInt(10)).fold()).select(column.values).unfold().order(scope.local),
  ],
  'g_V_group_by_byXout_label_tailX10X_foldX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group().by().by(Anon.out().label().tail(GInt(10)).fold()).select(column.values).unfold().order(scope.local),
  ],
  'g_V_groupXaX_byXnameX_by_selectXaX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by('name').by().select('a').count(scope.local),
  ],
  'g_V_localXgroupXaX_byXnameX_by_selectXaX_countXlocalXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.group('a').by('name').by().select('a').count(scope.local)),
  ],
  'g_V_group_byXvaluesXnameXX_byXboth_countX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(Anon.values('name')).by(Anon.both().count()),
  ],
  'g_V_outXcreatedX_groupCount_byXnameX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').groupCount().by('name'),
  ],
  'g_V_groupCount_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().groupCount().by('age'),
  ],
  'g_withStrategiesXProductiveByStrategyX_V_groupCount_byXageX': <Function>[
    (GraphTraversalSource g) => g.withStrategies(ProductiveByStrategy()).V().groupCount().by('age'),
  ],
  'g_V_outXcreatedX_name_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().out('created').values('name').groupCount(),
  ],
  'g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').groupCount('a').by('name').cap('a'),
  ],
  'g_V_outXcreatedX_name_groupCountXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').values('name').groupCount('a').cap('a'),
  ],
  'g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out().groupCount('a').by('name')).times(GInt(2)).cap('a'),
  ],
  'g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name': <Function>[
    (GraphTraversalSource g) => g.V().both().groupCount('a').by(t.label).as_('b').barrier().where(Anon.select('a').select('software').is_(P.gt(GInt(2)))).select('b').values('name'),
  ],
  'g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.out('knows'), Anon.out('created').in_('created')).groupCount().select(column.values).unfold().sum(),
  ],
  'g_V_hasXnoX_groupCount': <Function>[
    (GraphTraversalSource g) => g.V().has('no').groupCount(),
  ],
  'g_V_hasXnoX_groupCountXaX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('no').groupCount('a').cap('a'),
  ],
  'g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX': <Function>[
    (GraphTraversalSource g) => g.V().union(Anon.repeat(Anon.out()).times(GInt(2)).groupCount('m').by('lang'), Anon.repeat(Anon.in_()).times(GInt(2)).groupCount('m').by('name')).cap('m'),
  ],
  'g_V_outXcreatedX_groupCountXxX_capXxX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').groupCount('x').cap('x'),
  ],
  'g_V_groupCount_byXbothE_countX': <Function>[
    (GraphTraversalSource g) => g.V().groupCount().by(Anon.bothE().count()),
  ],
  'g_V_both_localXgroupCountXaXX_out_capXaX_selectXkeysX_unfold_both_localXgroupCountXaXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().both().local(Anon.groupCount('a')).out().cap('a').select(column.keys).unfold().both().local(Anon.groupCount('a')).cap('a'),
  ],
  'g_V_hasXperson_name_markoX_bothXknowsX_groupCount_byXvaluesXnameX_foldX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', 'marko').both('knows').groupCount().by(Anon.values('name').fold()),
  ],
  'g_V_outXcreatedX_groupCount_byXnameX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').groupCount().by('name').by('age'),
  ],
  'g_V_outXcreatedX_groupCountXxX_byXnameX_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().out('created').groupCount('x').by('name').by('age'),
  ],
  'g_V_groupCountXaX_selectXaX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().groupCount('a').select('a').count(scope.local),
  ],
  'g_V_localXgroupCountXaX_selectXaX_countXlocalXX': <Function>[
    (GraphTraversalSource g) => g.V().local(Anon.groupCount('a').select('a').count(scope.local)),
  ],
  'g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().values('name').inject('daniel').as_('a').map_(Anon.length()).path(),
  ],
  'g_injectXnull_1_3_nullX': <Function>[
    (GraphTraversalSource g) => g.inject(null, GInt(1), GInt(3), null),
  ],
  'g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(10), GInt(20), null, GInt(20), GInt(10), GInt(10)).groupCount('x').dedup().as_('y').project('a', 'b').by().by(Anon.select('x').select(Anon.select('y'))),
  ],
  'g_injectXname_marko_age_nullX_selectXname_ageX': <Function>[
    (GraphTraversalSource g) => g.inject({'name': 'marko', 'age': null}).select('name', 'age'),
  ],
  'g_injectXnull_nullX': <Function>[
    (GraphTraversalSource g) => g.inject(null, null),
  ],
  'g_injectXnullX': <Function>[
    (GraphTraversalSource g) => g.inject(null),
  ],
  'g_inject': <Function>[
    (GraphTraversalSource g) => g.inject(),
  ],
  'g_VX1X_valuesXageX_injectXnull_nullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1).values('age').inject(null, null),
  ],
  'g_VX1X_valuesXageX_injectXnullX': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1).values('age').inject(null),
  ],
  'g_VX1X_valuesXageX_inject': <Function>[
    (GraphTraversalSource g, {dynamic xx1}) => g.V(xx1).values('age').inject(),
  ],
  'g_injectXnull_1_3_nullX_asXaX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.inject(null, GInt(1), GInt(3), null).as_('a').select('a'),
  ],
  'g_injectX1_3X_injectX100_300X': <Function>[
    (GraphTraversalSource g) => g.inject(GInt(1), GInt(3)).inject(GInt(100), GInt(300)),
  ],
  'g_injectX1_3_100_300X_list': <Function>[
    (GraphTraversalSource g) => g.inject([GInt(1), GInt(3), GInt(100), GInt(300)]),
  ],
  'g_injectX1_3_100_300X_set': <Function>[
    (GraphTraversalSource g) => g.inject(<dynamic>{GInt(1), GInt(3), GInt(100), GInt(300)}),
  ],
  'g_injectX1_1X_set': <Function>[
    (GraphTraversalSource g) => g.inject(<dynamic>{GInt(1), GInt(1)}),
  ],
  'g_io_readXkryoX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.kryo').read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_io_read_withXreader_gryoX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.kryo').with_(IO.reader, IO.gryo).read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_io_readXgraphsonX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.json').read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_io_read_withXreader_graphsonX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.json').with_(IO.reader, IO.graphson).read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_io_readXgraphmlX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.xml').read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_io_read_withXreader_graphmlX': <Function>[
    (GraphTraversalSource g) => g.io('data/tinkerpop-modern.xml').with_(IO.reader, IO.graphml).read(),
    (GraphTraversalSource g) => g.V(),
    (GraphTraversalSource g) => g.E(),
  ],
  'g_withSackX127bX_injectX1bX_sackXsumX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GByte(127)).inject(GByte(1)).sack(operator_.sum).sack(),
  ],
  'g_withSackX32767sX_injectX1sX_sackXsumX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GShort(32767)).inject(GShort(1)).sack(operator_.sum).sack(),
  ],
  'g_withSackX2147483647iX_injectX1iX_sackXsumX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(2147483647)).inject(GInt(1)).sack(operator_.sum).sack(),
  ],
  'g_withSackX1_7976931348623157E_308dX_injectX1_7976931348623157E_308dX_sackXsumX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(1.7976931348623157e+308)).inject(GDouble(1.7976931348623157e+308)).sack(operator_.sum).sack(),
  ],
  'g_withSackX_128bX_injectX1bX_sackXminusX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GByte(-128)).inject(GByte(1)).sack(operator_.minus).sack(),
  ],
  'g_withSackX_32768sX_injectX1sX_sackXminusX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GShort(-32768)).inject(GShort(1)).sack(operator_.minus).sack(),
  ],
  'g_withSackX_2147483648iX_injectX1iX_sackXminusX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(-2147483648)).inject(GInt(1)).sack(operator_.minus).sack(),
  ],
  'g_withSackX_1_7976931348623157E_308dX_injectX1_7976931348623157E_308dX_sackXminusX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(-1.7976931348623157e+308)).inject(GDouble(1.7976931348623157e+308)).sack(operator_.minus).sack(),
  ],
  'g_withSackX127bX_injectX2bX_sackXmultX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GByte(127)).inject(GByte(2)).sack(operator_.mult).sack(),
  ],
  'g_withSackX32767sX_injectX2sX_sackXmultX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GShort(32767)).inject(GShort(2)).sack(operator_.mult).sack(),
  ],
  'g_withSackX2147483647iX_injectX2iX_sackXmultX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(2147483647)).inject(GInt(2)).sack(operator_.mult).sack(),
  ],
  'g_withSackX1_7976931348623157E_308dX_injectX2dX_sackXmultX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(1.7976931348623157e+308)).inject(GDouble(2)).sack(operator_.mult).sack(),
  ],
  'g_withSackX127bX_injectX0_5fX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GByte(127)).inject(GFloat(0.5)).sack(operator_.div).sack(),
  ],
  'g_withSackX32767sX_injectX0_5fX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GShort(32767)).inject(GFloat(0.5)).sack(operator_.div).sack(),
  ],
  'g_withSackX2147483647iX_injectX0_5fX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(2147483647)).inject(GFloat(0.5)).sack(operator_.div).sack(),
  ],
  'g_withSackX1_7976931348623157E_308dX_injectX0_5dX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(1.7976931348623157e+308)).inject(GDouble(0.5)).sack(operator_.div).sack(),
  ],
  'g_withSackX_128bX_injectX_1bX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GByte(-128)).inject(GByte(-1)).sack(operator_.div).sack(),
  ],
  'g_withSackX_32768sX_injectX_1sX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GShort(-32768)).inject(GShort(-1)).sack(operator_.div).sack(),
  ],
  'g_withSackX_2147483648iX_injectX_1iX_sackXdivX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(-2147483648)).inject(GInt(-1)).sack(operator_.div).sack(),
  ],
  'g_withSackXhelloX_V_outE_sackXassignX_byXlabelX_inV_sack': <Function>[
    (GraphTraversalSource g) => g.withSack('hello').V().outE().sack(operator_.assign).by(t.label).inV().sack(),
  ],
  'g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(0.0)).V().outE().sack(operator_.sum).by('weight').inV().sack().sum(),
  ],
  'g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GDouble(0.0)).V().repeat(Anon.outE().sack(operator_.sum).by('weight').inV()).times(GInt(2)).sack(),
  ],
  'g_withBulkXfalseX_withSackX1_sumX_VX1X_localXoutEXknowsX_barrierXnormSackX_inVX_inXknowsX_barrier_sack': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withBulk(false).withSack(GDouble(1.0), operator_.sum).V(vid1).local(Anon.outE('knows').barrier(barrier.normSack).inV()).in_('knows').barrier().sack(),
  ],
  'g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack': <Function>[
    (GraphTraversalSource g) => g.withBulk(false).withSack(GInt(1), operator_.sum).V().out().barrier().sack(),
  ],
  'g_withSackX1_sumX_VX1X_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.withSack(GDouble(1.0), operator_.sum).V(vid1).local(Anon.out('knows').barrier(barrier.normSack)).in_('knows').barrier().sack(),
  ],
  'g_V_sackXassignX_byXageX_sack': <Function>[
    (GraphTraversalSource g) => g.V().sack(operator_.assign).by('age').sack(),
  ],
  'g_withSackXBigInteger_TEN_powX1000X_assignX_V_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(BigInt.parse('10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), operator_.assign).V().local(Anon.out('knows').barrier(barrier.normSack)).in_('knows').barrier().sack(),
  ],
  'g_withSackX2X_V_sackXdivX_byXconstantX4_0XX_sack': <Function>[
    (GraphTraversalSource g) => g.withSack(GInt(2)).V().sack(operator_.div).by(Anon.constant(GDouble(4.0))).sack(),
  ],
  'g_V_sackXassignX_byXageX_byXnameX_sack': <Function>[
    (GraphTraversalSource g) => g.V().sack(operator_.assign).by('age').by('name').sack(),
  ],
  'g_V_sideEffectXidentityX': <Function>[
    (GraphTraversalSource g) => g.V().sideEffect(Anon.identity()),
  ],
  'g_V_sideEffectXidentity_valuesXnameXX': <Function>[
    (GraphTraversalSource g) => g.V().sideEffect(Anon.identity().values('name')),
  ],
  'g_V_sideEffectXpropertyXsingle_age_22X': <Function>[
    (GraphTraversalSource g) => g.addV('person').property(cardinality.single, 'age', GInt(21)),
    (GraphTraversalSource g) => g.V().sideEffect(Anon.property(cardinality.single, 'age', GInt(22))),
    (GraphTraversalSource g) => g.V().has('age', GInt(21)),
    (GraphTraversalSource g) => g.V().has('age', GInt(22)),
  ],
  'g_V_group_byXvaluesXnameX_sideEffectXconstantXzyxXX_substringX1XX_byXconstantX1X_sideEffectXconstantXxyzXXX': <Function>[
    (GraphTraversalSource g) => g.V().group().by(Anon.values('name').sideEffect(Anon.constant('zyx')).substring(GInt(0), GInt(1))).by(Anon.constant(GInt(1)).sideEffect(Anon.constant('xyz'))),
  ],
  'g_withSideEffectXx_setX_V_both_both_sideEffectXlocalXaggregateXxX_byXnameXX_capXxX_unfold': <Function>[
    (GraphTraversalSource g) => g.withSideEffect('x', <dynamic>{}).V().both().both().sideEffect(Anon.local(Anon.aggregate('x').by('name'))).cap('x').unfold(),
  ],
  'g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('age').groupCount('a').by('name').out().cap('a'),
  ],
  'g_V_groupXaX_byXageX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by('age').cap('a'),
  ],
  'g_V_groupXaX_byXnameX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by('name').cap('a'),
  ],
  'g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('lang').group('a').by('lang').by('name').out().cap('a'),
  ],
  'g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.out().group('a').by('name').by(Anon.count())).times(GInt(2)).cap('a'),
  ],
  'g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by(t.label).by(Anon.outE().values('weight').sum()).cap('a'),
  ],
  'g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.both('followedBy')).times(GInt(2)).group('a').by('songType').by(Anon.count()).cap('a'),
  ],
  'g_V_groupXaX_byXvaluesXnameX_substringX1XX_byXconstantX1XX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by(Anon.values('name').substring(GInt(0), GInt(1))).by(Anon.constant(GInt(1))).cap('a'),
  ],
  'g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('song').group('a').by('name').by(Anon.properties().groupCount().by(t.label)).out().cap('a'),
  ],
  'g_V_hasLabelXpersonX_asXpX_outXcreatedX_groupXaX_byXnameX_byXselectXpX_valuesXageX_sumX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().hasLabel('person').as_('p').out('created').group('a').by('name').by(Anon.select('p').values('age').sum()).cap('a'),
  ],
  'g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX': <Function>[
    (GraphTraversalSource g) => g.V().group('m').by('name').by(Anon.in_('knows').values('name')).cap('m'),
  ],
  'g_V_groupXmX_byXlabelX_byXlabel_countX_capXmX': <Function>[
    (GraphTraversalSource g) => g.V().group('m').by(Anon.label()).by(Anon.label().count()).cap('m'),
  ],
  'g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().choose(Anon.has(t.label, 'person'), Anon.values('age').groupCount('a'), Anon.values('name').groupCount('b')).cap('a', 'b').unfold(),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_groupXaX_by_byXout_orderX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group('a').by().by(Anon.out().order()).cap('a'),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_groupXaX_by_byXout_order_countX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group('a').by().by(Anon.out().order().count()).cap('a'),
  ],
  'g_V_hasXperson_name_withinXvadas_peterXX_groupXaX_by_byXout_order_fold_countXlocalXX_capXaX': <Function>[
    (GraphTraversalSource g) => g.V().has('person', 'name', P.within('vadas', 'peter')).group('a').by().by(Anon.out().order().fold().count(scope.local)).cap('a'),
  ],
  'g_V_groupXaX_by_byXout_label_foldX_capXaX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by().by(Anon.out().label().fold()).cap('a').select(column.values).unfold().order(scope.local),
  ],
  'g_V_groupXaX_by_byXout_label_dedup_foldX_capXaX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by().by(Anon.out().label().dedup().fold()).cap('a').select(column.values).unfold().order(scope.local),
  ],
  'g_V_groupXaX_by_byXout_label_limitX0X_foldX_capXaX_selectXvaluesX_unfold': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by().by(Anon.out().label().limit(GInt(0)).fold()).cap('a').select(column.values).unfold(),
  ],
  'g_V_groupXaX_by_byXout_label_limitX10X_foldX_capXaX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by().by(Anon.out().label().limit(GInt(10)).fold()).cap('a').select(column.values).unfold().order(scope.local),
  ],
  'g_V_groupXaX_by_byXout_label_tailX10X_foldX_capXaX_selectXvaluesX_unfold_orderXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().group('a').by().by(Anon.out().label().tail(GInt(10)).fold()).cap('a').select(column.values).unfold().order(scope.local),
  ],
  'g_VX1X_outEXknowsX_subgraphXsgX_name_capXsgX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE('knows').subgraph('sg').values('name').cap('sg'),
  ],
  'g_V_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup_capXsgX': <Function>[
    (GraphTraversalSource g) => g.V().repeat(Anon.bothE('created').subgraph('sg').outV()).times(GInt(5)).values('name').dedup().cap('sg'),
  ],
  'g_V_outEXnoexistX_subgraphXsgXcapXsgX': <Function>[
    (GraphTraversalSource g) => g.V().outE('noexist').subgraph('sg').cap('sg'),
  ],
  'g_E_hasXweight_0_5X_subgraphXaX_selectXaX': <Function>[
    (GraphTraversalSource g) => g.E().has('weight', GDouble(0.4)).subgraph('a').select('a'),
  ],
  'g_VX1X_out_out_tree_byXnameX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree().by('name'),
  ],
  'g_VX1X_out_out_tree': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree(),
  ],
  'g_V_out_tree_byXageX': <Function>[
    (GraphTraversalSource g) => g.V().out().tree().by('age'),
  ],
  'g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree('a').by('name').both().both().cap('a'),
  ],
  'g_VX1X_out_out_treeXaX_both_both_capXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree('a').both().both().cap('a'),
  ],
  'g_VX1X_out_out_tree_byXlabelX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree().by(t.label),
  ],
  'g_VX1X_out_out_treeXaX_byXlabelX_both_both_capXaX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).out().out().tree('a').by(t.label).both().both().cap('a'),
  ],
  'g_VX1X_out_out_out_tree': <Function>[
    (GraphTraversalSource g) => g.V().out().out().out().tree(),
  ],
  'g_VX1X_outE_inV_bothE_otherV_tree': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().inV().bothE().otherV().tree(),
  ],
  'g_VX1X_outE_inV_bothE_otherV_tree_byXnameX_byXlabelX': <Function>[
    (GraphTraversalSource g, {dynamic vid1}) => g.V(vid1).outE().inV().bothE().otherV().tree().by('name').by(t.label),
  ],
  'g_V_out_treeXaX_selectXaX_countXlocalX': <Function>[
    (GraphTraversalSource g) => g.V().out().tree('a').select('a').count(scope.local),
  ],
  'g_V_out_order_byXnameX_localXtreeXaX_selectXaX_countXlocalXX': <Function>[
    (GraphTraversalSource g) => g.V().out().local(Anon.tree('a').select('a').count(scope.local)),
  ],
  'g_io_writeXkryoX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern-v3.kryo').write(),
  ],
  'g_io_write_withXwriter_gryoX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern-v3.kryo').with_(IO.writer, IO.gryo).write(),
  ],
  'g_io_writeXgraphsonX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern-v3.json').write(),
  ],
  'g_io_write_withXwriter_graphsonX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern-v3.json').with_(IO.writer, IO.graphson).write(),
  ],
  'g_io_writeXgraphmlX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern.xml').write(),
  ],
  'g_io_write_withXwriter_graphmlX': <Function>[
    (GraphTraversalSource g) => g.io('tinkerpop-modern.xml').with_(IO.writer, IO.graphml).write(),
  ],
};
