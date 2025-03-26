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

/**
 * @author Igor Ostapenko
 */

import { des_title } from './utils.js';
import assert from 'assert';
import { bulkSetSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

const { from, concat } = Buffer;

describe('GraphBinary.BulkSetSerializer', () => {

  const type_code =  from([0x2A]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x2A, 0x01],          av:null },
    { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00], av:[] },
    { v:null,      fq:1, b:[0x2A, 0x01] },
    { v:null,      fq:0, b:[0x00,0x00,0x00,0x00], av:[] },

    { v:[],
      b:[0x00,0x00,0x00,0x00,
      ]
    },

    { v:[ 'A' ],
      b:[0x00,0x00,0x00,0x01,
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
      ]
    },

    { v:[ 'A', 'A' ],
      b:[0x00,0x00,0x00,0x01,
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x02, // bulk
      ]
    },

    { v:[ ],
      b:[0x00,0x00,0x00,0x01,
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00, // bulk
      ]
    },

    { v:[ 'A', 'A' ],
      b:[0x00,0x00,0x00,0x02,
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
      ]
    },

    { v:[ 15, 'B' ],
      b:[0x00,0x00,0x00,0x02,
        0x01,0x00, 0x00,0x00,0x00,0x0F, // 15
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x42, // 'B'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
      ]
    },

    { v:[ -1, -1, -1, 'B', [], [] ],
      b:[0x00,0x00,0x00,0x03,
        0x01,0x00, 0xFF,0xFF,0xFF,0xFF, // -1
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x03, // bulk
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x42, // 'B'
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01, // bulk
        0x09,0x00, 0x00,0x00,0x00,0x00, // []
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x02, // bulk
      ]
    },

    { des:1, err:/buffer is missing/,                 fq:1, b:undefined },
    { des:1, err:/buffer is missing/,                 fq:0, b:undefined },
    { des:1, err:/buffer is missing/,                 fq:1, b:null },
    { des:1, err:/buffer is missing/,                 fq:0, b:null },
    { des:1, err:/buffer is empty/,                   fq:1, b:[] },
    { des:1, err:/buffer is empty/,                   fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x09] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0xA2] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x29] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x2B] },
    { des:1, err:/unexpected {type_code}/,            fq:1, b:[0x20] },

    { des:1, err:/{value_flag} is missing/,           fq:1, b:[0x2A] },
    { des:1, err:/unexpected {value_flag}/,           fq:1, b:[0x2A,0x10] },
    { des:1, err:/unexpected {value_flag}/,           fq:1, b:[0x2A,0x02] },
    { des:1, err:/unexpected {value_flag}/,           fq:1, b:[0x2A,0x0F] },
    { des:1, err:/unexpected {value_flag}/,           fq:1, b:[0x2A,0xFF] },

    { des:1, err:/{length} is less than zero/,              b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,              b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/{item_0}: bulk is less than zero/,        b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x00, 0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{item_0}: bulk is less than zero/,        b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x00, 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { des:1, err:/{item_0}: bulk is greater than 2\^32-1/,  b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x01,0x00,0x00,0x00,0x00] },
  ];

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => bulkSetSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => bulkSetSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => bulkSetSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( bulkSetSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( bulkSetSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( bulkSetSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

});
