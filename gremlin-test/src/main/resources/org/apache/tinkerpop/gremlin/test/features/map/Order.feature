# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassMap @StepOrder
Feature: Step - order()

  Scenario: g_V_name_order
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_name_order_byXa1_b1X_byXb2_a2X
    Given the modern graph
    And using the parameter c1 defined as "c[a, b -> a.substring(1, 2).compareTo(b.substring(1, 2))]"
    And using the parameter c2 defined as "c[a, b -> b.substring(2, 3).compareTo(a.substring(2, 3))]"
    And the traversal of
      """
      g.V().values("name").order().by(c1).by(c2)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | marko  |
      | vadas  |
      | peter  |
      | ripple |
      | josh   |
      | lop    |

  Scenario: g_V_order_byXname_ascX_name
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name", Order.asc).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_order_byXnameX_name
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name").values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_outE_order_byXweight_descX_weight
    Given the modern graph
    And the traversal of
      """
      g.V().outE().order().by("weight", Order.desc).values("weight")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[1.0].d |
      | d[1.0].d |
      | d[0.5].d |
      | d[0.4].d |
      | d[0.4].d |
      | d[0.2].d |

  Scenario: g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").order().by(Order.shuffle).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |

  Scenario: g_V_both_hasLabelXpersonX_order_byXage_descX_limitX5X_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().hasLabel("person").order().by("age", Order.desc).limit(5).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | peter  |
      | josh  |
      | josh  |
      | josh |
      | marko  |

  Scenario: g_V_properties_order_byXkey_descX_key
    Given the modern graph
    And the traversal of
      """
      g.V().properties().order().by(T.key, Order.desc).key()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | name   |
      | name   |
      | name   |
      | name   |
      | name   |
      | name   |
      | lang   |
      | lang   |
      | age    |
      | age    |
      | age    |
      | age    |

  Scenario: g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name
    Given the modern graph
    And using the parameter l1 defined as "c[it.value('age')]"
    And the traversal of
      """
      g.V().hasLabel("person").order().by(l1, Order.desc).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | peter  |
      | josh   |
      | marko  |
      | vadas  |

  Scenario: g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_orderXlocalX_byXvaluesX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").group().by("name").by(__.outE().values("weight").sum()).order(Scope.local).by(Column.values)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"peter":"d[0.2].d","josh":"d[1.4].d","marko":"d[1.9].d"}] |

  Scenario: g_V_mapXbothE_weight_foldX_order_byXsumXlocalX_descX_byXcountXlocalX_descX
    Given the modern graph
    And the traversal of
      """
      g.V().map(__.bothE().values("weight").order().by(Order.asc).fold()).order().by(__.sum(Scope.local), Order.desc).by(__.count(Scope.local), Order.desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | l[d[0.4].d,d[1.0].d,d[1.0].d] |
      | l[d[0.4].d,d[0.5].d,d[1.0].d] |
      | l[d[0.2].d,d[0.4].d,d[0.4].d] |
      | l[d[1.0].d]                   |
      | l[d[0.5].d]                   |
      | l[d[0.2].d]                   |

  Scenario: g_V_group_byXlabelX_byXname_order_byXdescX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.values("name").order().by(Order.desc).fold())
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"software":"l[ripple,lop]","person":"l[vadas,peter,marko,josh]"}]  |

  Scenario: g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_descX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").group().by("name").by(__.outE().values("weight").sum()).unfold().order().by(Column.values, Order.desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"marko":"d[1.9].d"}]  |
      | m[{"josh":"d[1.4].d"}]  |
      | m[{"peter":"d[0.2].d"}]  |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_descX_byXselectXvX_nameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").map(__.bothE().values("weight").fold()).sum(Scope.local).as("s").select("v", "s").order().by(__.select("s"), Order.desc).by(__.select("v").values("name"))
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"v":"v[josh]","s":"d[2.4].d"}]  |
      | m[{"v":"v[marko]","s":"d[1.9].d"}]  |
      | m[{"v":"v[lop]","s":"d[1.0].d"}]  |
      | m[{"v":"v[ripple]","s":"d[1.0].d"}]  |
      | m[{"v":"v[vadas]","s":"d[0.5].d"}]  |
      | m[{"v":"v[peter]","s":"d[0.2].d"}]  |

  Scenario: g_V_hasLabelXpersonX_fold_orderXlocalX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").fold().order(Scope.local).by("age")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | l[v[vadas],v[marko],v[josh],v[peter]] |

  Scenario: g_V_both_hasLabelXpersonX_order_byXage_descX_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().hasLabel("person").order().by("age", Order.desc).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | peter  |
      | josh   |
      | josh   |
      | josh   |
      | marko  |
      | marko  |
      | marko  |
      | vadas  |

  Scenario: g_V_order_byXoutE_count_descX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().order().by(__.outE().count(), Order.desc).by("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[marko] |
      | v[josh]   |
      | v[peter] |
      | v[lop] |
      | v[ripple] |
      | v[vadas] |

  Scenario: g_V_hasLabelXpersonX_order_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").order().by("age")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[vadas] |
      | v[marko] |
      | v[josh]   |
      | v[peter] |

  Scenario: g_V_order_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("age")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[vadas] |
      | v[marko] |
      | v[josh]   |
      | v[peter] |

  Scenario: g_V_fold_orderXlocalX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().order(local).by("age")
      """
    When iterated next
    Then the result should be ordered
      | result |
      | v[vadas] |
      | v[marko] |
      | v[josh]   |
      | v[peter] |


  Scenario: g_V_fold_orderXlocalX_byXage_descX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().order(local).by("age", desc)
      """
    When iterated next
    Then the result should be ordered
      | result |
      | v[peter] |
      | v[josh] |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_orXhasLabelXpersonX_hasXsoftware_name_lopXX_order_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().or(hasLabel("person"),has("software","name","lop")).order().by("age")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[vadas] |
      | v[marko] |
      | v[josh]   |
      | v[peter] |

  # tests order().by() where a property isn't present to ensure null comes first
  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_orXhasLabelXpersonX_hasXsoftware_name_lopXX_order_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().or(hasLabel("person"),has("software","name","lop")).order().by("age")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[lop] |
      | v[vadas] |
      | v[marko] |
      | v[josh]   |
      | v[peter] |

  Scenario: g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX
    Given the modern graph
    And using the parameter v1 defined as "v[marko]"
    And using the parameter l1 defined as "c[['1':it.get().value('age'),'2':it.get().value('age')*2,'3':it.get().value('age')*3,'4':it.get().value('age')]]"
    And the traversal of
      """
      g.V(v1).hasLabel("person").map(l1).order(Scope.local).by(Column.values, Order.desc).by(Column.keys, Order.asc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"3":"d[87].i","2":"d[58].i","1":"d[29].i","4":"d[29].i"}] |

  Scenario: g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX
    Given the grateful graph
    And the traversal of
      """
      g.V().has("song", "name", "OH BOY").out("followedBy").out("followedBy").order().by("performances").by("songType", desc).by("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[BARBRY ALLEN] |
      | v[CHANTING BY THE GYOTO MONKS] |
      | v[FUNICULI FUNICULA] |
      | v[GIMME SOME LOVIN] |
      | v[GOOD TIME BLUES] |
      | v[LUCY IN THE SKY] |
      | v[MAYBE YOU KNOW HOW I FEEL] |
      | v[MOJO] |
      | v[OLLIN ARRAGEED] |
      | v[QUINN THE ESKIMO] |
      | v[SATISFACTION] |
      | v[SILENT WAY JAM] |
      | v[SPACE] |
      | v[STRONGER THAN DIRT] |
      | v[THE BOXER] |
      | v[THIS COULD BE THE LAST TIME] |
      | v[TOM THUMB BLUES] |
      | v[CHINESE BONES] |
      | v[LOVE THE ONE YOURE WITH] |
      | v[BLACK QUEEN] |
      | v[OH BOY] |
      | v[BLUES FOR ALLAH] |
      | v[IF I HAD THE WORLD TO GIVE] |
      | v[HEY JUDE] |
      | v[ITS ALL TOO MUCH] |
      | v[WILLIE AND THE HAND JIVE] |
      | v[WHY DONT WE DO IT IN THE ROAD] |
      | v[UNBROKEN CHAIN] |
      | v[DONT NEED LOVE] |
      | v[NOBODYS FAULT BUT MINE] |
      | v[HEAVEN HELP THE FOOL] |
      | v[BLOW AWAY] |
      | v[JAM] |
      | v[SUNSHINE DAYDREAM] |
      | v[I WILL TAKE YOU HOME] |
      | v[SAMBA IN THE RAIN] |
      | v[ON THE ROAD AGAIN] |
      | v[SPANISH JAM] |
      | v[EASY TO LOVE YOU] |
      | v[DEATH DONT HAVE NO MERCY] |
      | v[SPOONFUL] |
      | v[CAUTION] |
      | v[THE RACE IS ON] |
      | v[SMOKESTACK LIGHTNING] |
      | v[COMES A TIME] |
      | v[STANDING ON THE MOON] |
      | v[KNOCKING ON HEAVENS DOOR] |
      | v[PICASSO MOON] |
      | v[FOOLISH HEART] |
      | v[WAY TO GO HOME] |
      | v[THE ELEVEN] |
      | v[VICTIM OR THE CRIME] |
      | v[PASSENGER] |
      | v[PASSENGER] |
      | v[MY BROTHER ESAU] |
      | v[HELP ON THE WAY] |
      | v[LAZY LIGHTNING] |
      | v[CHINA DOLL] |
      | v[ME AND BOBBY MCGEE] |
      | v[ALL ALONG THE WATCHTOWER] |
      | v[CRYPTICAL ENVELOPMENT] |
      | v[ALABAMA GETAWAY] |
      | v[CRAZY FINGERS] |
      | v[CRAZY FINGERS] |
      | v[WHEN I PAINT MY MASTERPIECE] |
      | v[LOST SAILOR] |
      | v[LOST SAILOR] |
      | v[BLACK THROATED WIND] |
      | v[IT MUST HAVE BEEN THE ROSES] |
      | v[IT MUST HAVE BEEN THE ROSES] |
      | v[BOX OF RAIN] |
      | v[SHAKEDOWN STREET] |
      | v[SHAKEDOWN STREET] |
      | v[IKO IKO] |
      | v[IKO IKO] |
      | v[FEEL LIKE A STRANGER] |
      | v[TOUCH OF GREY] |
      | v[TOUCH OF GREY] |
      | v[BROKEDOWN PALACE] |
      | v[HELL IN A BUCKET] |
      | v[DARK STAR] |
      | v[DARK STAR] |
      | v[FRANKLINS TOWER] |
      | v[SAINT OF CIRCUMSTANCE] |
      | v[SAINT OF CIRCUMSTANCE] |
      | v[THE MUSIC NEVER STOPPED] |
      | v[COLD RAIN AND SNOW] |
      | v[FIRE ON THE MOUNTAIN] |
      | v[MORNING DEW] |
      | v[THE WHEEL] |
      | v[THROWING STONES] |
      | v[I NEED A MIRACLE] |
      | v[I NEED A MIRACLE] |
      | v[ALTHEA] |
      | v[LITTLE RED ROOSTER] |
      | v[LET IT GROW] |
      | v[LET IT GROW] |
      | v[GOING DOWN THE ROAD FEELING BAD] |
      | v[BIRDSONG] |
      | v[TERRAPIN STATION] |
      | v[TERRAPIN STATION] |
      | v[MAMA TRIED] |
      | v[FRIEND OF THE DEVIL] |
      | v[FRIEND OF THE DEVIL] |
      | v[SCARLET BEGONIAS] |
      | v[SCARLET BEGONIAS] |
      | v[BEAT IT ON DOWN THE LINE] |
      | v[HES GONE] |
      | v[STELLA BLUE] |
      | v[UNCLE JOHNS BAND] |
      | v[UNCLE JOHNS BAND] |
      | v[CASSIDY] |
      | v[ONE MORE SATURDAY NIGHT] |
      | v[BLACK PETER] |
      | v[BROWN EYED WOMEN] |
      | v[SUGAREE] |
      | v[SAMSON AND DELILAH] |
      | v[SAMSON AND DELILAH] |
      | v[EYES OF THE WORLD] |
      | v[EYES OF THE WORLD] |
      | v[EL PASO] |
      | v[ESTIMATED PROPHET] |
      | v[BERTHA] |
      | v[WHARF RAT] |
      | v[BIG RIVER] |
      | v[LOOKS LIKE RAIN] |
      | v[AROUND AND AROUND] |
      | v[PROMISED LAND] |
      | v[GOOD LOVING] |
      | v[MEXICALI BLUES] |
      | v[NEW MINGLEWOOD BLUES] |
      | v[JACK STRAW] |
      | v[JACK STRAW] |
      | v[TRUCKING] |
      | v[TRUCKING] |
      | v[NOT FADE AWAY] |
      | v[CHINA CAT SUNFLOWER] |
      | v[CHINA CAT SUNFLOWER] |
      | v[PLAYING IN THE BAND] |
      | v[PLAYING IN THE BAND] |
      | v[THE OTHER ONE] |
      | v[SUGAR MAGNOLIA] |
      | v[SUGAR MAGNOLIA] |
      | v[ME AND MY UNCLE] |

  Scenario: g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").order().by("performances", desc).by("name").range(110, 120).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | WANG DANG DOODLE |
      | THE ELEVEN |
      | WAY TO GO HOME |
      | FOOLISH HEART |
      | GIMME SOME LOVING |
      | DUPREES DIAMOND BLUES |
      | CORRINA |
      | PICASSO MOON |
      | KNOCKING ON HEAVENS DOOR |
      | MEMPHIS BLUES |

  Scenario: g_VX1X_elementMap_orderXlocalX_byXkeys_descXunfold
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).elementMap().order(Scope.local).by(Column.keys, Order.desc).unfold()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"name":"marko"}] |
      | m[{"t[label]":"person"}] |
      | m[{"t[id]":"v[marko].id"}] |
      | m[{"age":29}] |

  Scenario: g_VX1X_elementMap_orderXlocalX_byXkeys_ascXunfold
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).elementMap().order(Scope.local).by(Column.keys, Order.asc).unfold()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"age":29}] |
      | m[{"t[id]":"v[marko].id"}] |
      | m[{"t[label]":"person"}] |
      | m[{"name":"marko"}] |

  Scenario: g_VX1X_valuesXageX_orderXlocalX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).values("age").order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |