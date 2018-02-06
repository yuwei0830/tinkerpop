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
    And using the parameter l1 defined as "c[a, b -> a.substring(1, 2).compareTo(b.substring(1, 2))]"
    And using the parameter l2 defined as "c[a, b -> b.substring(2, 3).compareTo(a.substring(2, 3))]"
    And the traversal of
      """
      g.V().values("name").order().by(l1).by(l2)
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

  Scenario: g_V_order_byXname_incrX_name
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name", Order.incr).values("name")
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

  Scenario: g_V_outE_order_byXweight_decrX_weight
    Given the modern graph
    And the traversal of
      """
      g.V().outE().order().by("weight", Order.decr).values("weight")
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

  Scenario: g_V_order_byXname_a1_b1X_byXname_b2_a2X_name
    Given the modern graph
    And using the parameter l1 defined as "c[a, b -> a.substring(1, 2).compareTo(b.substring(1, 2))]"
    And using the parameter l2 defined as "c[a, b -> b.substring(2, 3).compareTo(a.substring(2, 3))]"
    And the traversal of
      """
      g.V().order().by("name", l1).by("name", l2).values("name")
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

  Scenario: g_V_both_hasLabelXpersonX_order_byXage_decrX_limitX5X_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().hasLabel("person").order().by("age", Order.decr).limit(5).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | peter  |
      | josh  |
      | josh  |
      | josh |
      | marko  |

  Scenario: g_V_properties_order_byXkey_decrX_key
    Given the modern graph
    And the traversal of
      """
      g.V().properties().order().by(T.key, Order.decr).key()
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

  Scenario: g_V_hasLabelXpersonX_order_byXvalueXageX__decrX_name
    Given the modern graph
    And using the parameter l1 defined as "c[it.value('age')]"
    And the traversal of
      """
      g.V().hasLabel("person").order().by(l1, Order.decr).values("name")
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
      | m[{"vadas":"d[0].d","peter":"d[0.2].d","josh":"d[1.4].d","marko":"d[1.9].d"}] |

  Scenario: g_V_localXbothE_weight_foldX_order_byXsumXlocalX_decrX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.bothE().values("weight").fold()).order().by(__.sum(Scope.local), Order.decr)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | l[d[1.0].f,d[0.4].f,d[1.0].f] |
      | l[d[0.4].f,d[0.5].f,d[1.0].f] |
      | l[d[0.4].f,d[0.4].f,d[0.2].f] |
      | l[d[1.0].f]                   |
      | l[d[0.5].f]                   |
      | l[d[0.2].f]                   |

  Scenario: g_V_group_byXlabelX_byXname_order_byXdecrX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.values("name").order().by(Order.decr).fold())
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"software":"l[ripple,lop]","person":"l[vadas,peter,marko,josh]"}]  |

  Scenario: g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_decrX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").group().by("name").by(__.outE().values("weight").sum()).unfold().order().by(Column.values, Order.decr)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"marko":"d[1.9].d"}]  |
      | m[{"josh":"d[1.4].d"}]  |
      | m[{"peter":"d[0.2].d"}]  |
      | m[{"vadas":"d[0].d"}]  |

  Scenario: g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_decrX
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").map(__.bothE().values("weight").fold()).sum(Scope.local).as("s").select("v", "s").order().by(__.select("s"), Order.decr)
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

  Scenario: g_V_both_hasLabelXpersonX_order_byXage_decrX_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().hasLabel("person").order().by("age", Order.decr).values("name")
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

  Scenario: g_V_order_byXoutE_count__decrX
    Given the modern graph
    And the traversal of
      """
      g.V().order().by(__.outE().count(), Order.decr)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[marko] |
      | v[josh]   |
      | v[peter] |
      | v[vadas] |
      | v[lop] |
      | v[ripple] |

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

  Scenario: g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_decrX_byXkeys_incrX
    Given the modern graph
    And using the parameter v1 defined as "v[marko]"
    And using the parameter l1 defined as "c[[1:it.get().value('age'),2:it.get().value('age')*2,3:it.get().value('age')*3,4:it.get().value('age')]]"
    And the traversal of
      """
      g.V(v1).hasLabel("person").map(l1).order(Scope.local).by(Column.values, Order.decr).by(Column.keys, Order.incr)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"3":"d[87].i","2":"d[58].i","1":"d[29].i","4":"d[29].i"}] |

