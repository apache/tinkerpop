/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

function shuffle(array) {
  const length = array == null ? 0 : array.length;
  if (!length) {
    return [];
  }
  let index = -1;
  const lastIndex = length - 1;
  while (++index < length) {
    const rand = index + Math.floor(Math.random() * (lastIndex - index + 1));
    const value = array[rand];
    array[rand] = array[index];
    array[index] = value;
  }
  return array;
}

export const createRandomColorGenerator = () => {
  const colors = [
    "#C62828",
    "#AD1457",
    "#6A1B9A",
    "#4527A0",
    "#283593",
    "#1565C0",
    "#0277BD",
    "#00838F",
    "#00695C",
    "#2E7D32",
    "#558B2F",
    "#9E9D24",
    "#F9A825",
    "#FF8F00",
    "#EF6C00",
    "#D84315",
    "#4E342E",
    "#424242",
    "#37474F",
  ];

  const colorMap = new Map();

  return (key) => {
    const storedColor = colorMap.get(key);

    if (storedColor !== undefined) {
      return storedColor;
    }

    const color = shuffle(colors).pop();

    colorMap.set(key, color);

    return color;
  };
};
