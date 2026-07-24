// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

class ResultSet<T> {
  final List<T> items;
  final Map<String, dynamic> attributes;

  const ResultSet(this.items, [this.attributes = const {}]);

  T operator [](int index) => items[index];

  int get length => items.length;

  bool get isEmpty => items.isEmpty;

  @override
  String toString() => 'ResultSet($items)';
}
