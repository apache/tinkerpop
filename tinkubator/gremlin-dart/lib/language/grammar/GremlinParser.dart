// Generated from gremlin-language/src/main/antlr4/Gremlin.g4 by ANTLR 4.13.2
// ignore_for_file: unused_import, unused_local_variable, prefer_single_quotes
import 'package:antlr4/antlr4.dart';

import 'GremlinVisitor.dart';
import 'GremlinBaseVisitor.dart';
const int RULE_queryList = 0, RULE_query = 1, RULE_emptyQuery = 2, RULE_traversalSource = 3, 
          RULE_transactionPart = 4, RULE_rootTraversal = 5, RULE_traversalSourceSelfMethod = 6, 
          RULE_traversalSourceSelfMethod_withBulk = 7, RULE_traversalSourceSelfMethod_withPath = 8, 
          RULE_traversalSourceSelfMethod_withSack = 9, RULE_traversalSourceSelfMethod_withSideEffect = 10, 
          RULE_traversalSourceSelfMethod_withStrategies = 11, RULE_traversalSourceSelfMethod_withoutStrategies = 12, 
          RULE_traversalSourceSelfMethod_with = 13, RULE_traversalSourceSpawnMethod = 14, 
          RULE_traversalSourceSpawnMethod_addE = 15, RULE_traversalSourceSpawnMethod_addV = 16, 
          RULE_traversalSourceSpawnMethod_E = 17, RULE_traversalSourceSpawnMethod_V = 18, 
          RULE_traversalSourceSpawnMethod_inject = 19, RULE_traversalSourceSpawnMethod_io = 20, 
          RULE_traversalSourceSpawnMethod_mergeV = 21, RULE_traversalSourceSpawnMethod_mergeE = 22, 
          RULE_traversalSourceSpawnMethod_call = 23, RULE_traversalSourceSpawnMethod_union = 24, 
          RULE_chainedTraversal = 25, RULE_nestedTraversal = 26, RULE_terminatedTraversal = 27, 
          RULE_traversalMethod = 28, RULE_traversalMethod_V = 29, RULE_traversalMethod_E = 30, 
          RULE_traversalMethod_addE = 31, RULE_traversalMethod_addV = 32, 
          RULE_traversalMethod_aggregate = 33, RULE_traversalMethod_all = 34, 
          RULE_traversalMethod_and = 35, RULE_traversalMethod_any = 36, 
          RULE_traversalMethod_as = 37, RULE_traversalMethod_asBool = 38, 
          RULE_traversalMethod_asDate = 39, RULE_traversalMethod_asNumber = 40, 
          RULE_traversalMethod_asString = 41, RULE_traversalMethod_barrier = 42, 
          RULE_traversalMethod_both = 43, RULE_traversalMethod_bothE = 44, 
          RULE_traversalMethod_bothV = 45, RULE_traversalMethod_branch = 46, 
          RULE_traversalMethod_by = 47, RULE_traversalMethod_call = 48, 
          RULE_traversalMethod_cap = 49, RULE_traversalMethod_choose = 50, 
          RULE_traversalMethod_coalesce = 51, RULE_traversalMethod_coin = 52, 
          RULE_traversalMethod_combine = 53, RULE_traversalMethod_concat = 54, 
          RULE_traversalMethod_conjoin = 55, RULE_traversalMethod_connectedComponent = 56, 
          RULE_traversalMethod_constant = 57, RULE_traversalMethod_count = 58, 
          RULE_traversalMethod_cyclicPath = 59, RULE_traversalMethod_dateAdd = 60, 
          RULE_traversalMethod_dateDiff = 61, RULE_traversalMethod_dedup = 62, 
          RULE_traversalMethod_difference = 63, RULE_traversalMethod_discard = 64, 
          RULE_traversalMethod_disjunct = 65, RULE_traversalMethod_drop = 66, 
          RULE_traversalMethod_element = 67, RULE_traversalMethod_elementMap = 68, 
          RULE_traversalMethod_emit = 69, RULE_traversalMethod_fail = 70, 
          RULE_traversalMethod_filter = 71, RULE_traversalMethod_flatMap = 72, 
          RULE_traversalMethod_fold = 73, RULE_traversalMethod_format = 74, 
          RULE_traversalMethod_from = 75, RULE_traversalMethod_group = 76, 
          RULE_traversalMethod_groupCount = 77, RULE_traversalMethod_has = 78, 
          RULE_traversalMethod_hasId = 79, RULE_traversalMethod_hasKey = 80, 
          RULE_traversalMethod_hasLabel = 81, RULE_traversalMethod_hasNot = 82, 
          RULE_traversalMethod_hasValue = 83, RULE_traversalMethod_id = 84, 
          RULE_traversalMethod_identity = 85, RULE_traversalMethod_in = 86, 
          RULE_traversalMethod_inE = 87, RULE_traversalMethod_intersect = 88, 
          RULE_traversalMethod_inV = 89, RULE_traversalMethod_index = 90, 
          RULE_traversalMethod_inject = 91, RULE_traversalMethod_is = 92, 
          RULE_traversalMethod_key = 93, RULE_traversalMethod_label = 94, 
          RULE_traversalMethod_length = 95, RULE_traversalMethod_limit = 96, 
          RULE_traversalMethod_local = 97, RULE_traversalMethod_loops = 98, 
          RULE_traversalMethod_lTrim = 99, RULE_traversalMethod_map = 100, 
          RULE_traversalMethod_match = 101, RULE_traversalMethod_math = 102, 
          RULE_traversalMethod_max = 103, RULE_traversalMethod_mean = 104, 
          RULE_traversalMethod_merge = 105, RULE_traversalMethod_mergeV = 106, 
          RULE_traversalMethod_mergeE = 107, RULE_traversalMethod_min = 108, 
          RULE_traversalMethod_none = 109, RULE_traversalMethod_not = 110, 
          RULE_traversalMethod_option = 111, RULE_traversalMethod_optional = 112, 
          RULE_traversalMethod_or = 113, RULE_traversalMethod_order = 114, 
          RULE_traversalMethod_otherV = 115, RULE_traversalMethod_out = 116, 
          RULE_traversalMethod_outE = 117, RULE_traversalMethod_outV = 118, 
          RULE_traversalMethod_pageRank = 119, RULE_traversalMethod_path = 120, 
          RULE_traversalMethod_peerPressure = 121, RULE_traversalMethod_product = 122, 
          RULE_traversalMethod_profile = 123, RULE_traversalMethod_project = 124, 
          RULE_traversalMethod_properties = 125, RULE_traversalMethod_property = 126, 
          RULE_traversalMethod_propertyMap = 127, RULE_traversalMethod_range = 128, 
          RULE_traversalMethod_read = 129, RULE_traversalMethod_repeat = 130, 
          RULE_traversalMethod_replace = 131, RULE_traversalMethod_reverse = 132, 
          RULE_traversalMethod_rTrim = 133, RULE_traversalMethod_sack = 134, 
          RULE_traversalMethod_sample = 135, RULE_traversalMethod_select = 136, 
          RULE_traversalMethod_shortestPath = 137, RULE_traversalMethod_sideEffect = 138, 
          RULE_traversalMethod_simplePath = 139, RULE_traversalMethod_skip = 140, 
          RULE_traversalMethod_split = 141, RULE_traversalMethod_subgraph = 142, 
          RULE_traversalMethod_substring = 143, RULE_traversalMethod_sum = 144, 
          RULE_traversalMethod_tail = 145, RULE_traversalMethod_timeLimit = 146, 
          RULE_traversalMethod_times = 147, RULE_traversalMethod_to = 148, 
          RULE_traversalMethod_toE = 149, RULE_traversalMethod_toLower = 150, 
          RULE_traversalMethod_toUpper = 151, RULE_traversalMethod_toV = 152, 
          RULE_traversalMethod_tree = 153, RULE_traversalMethod_trim = 154, 
          RULE_traversalMethod_unfold = 155, RULE_traversalMethod_union = 156, 
          RULE_traversalMethod_until = 157, RULE_traversalMethod_value = 158, 
          RULE_traversalMethod_valueMap = 159, RULE_traversalMethod_values = 160, 
          RULE_traversalMethod_where = 161, RULE_traversalMethod_with = 162, 
          RULE_traversalMethod_write = 163, RULE_traversalStrategy = 164, 
          RULE_configuration = 165, RULE_traversalScope = 166, RULE_traversalBarrier = 167, 
          RULE_traversalT = 168, RULE_traversalTShort = 169, RULE_traversalTLong = 170, 
          RULE_traversalMerge = 171, RULE_traversalOrder = 172, RULE_traversalDirection = 173, 
          RULE_traversalDirectionShort = 174, RULE_traversalDirectionLong = 175, 
          RULE_traversalCardinality = 176, RULE_traversalColumn = 177, RULE_traversalPop = 178, 
          RULE_traversalOperator = 179, RULE_traversalPick = 180, RULE_traversalDT = 181, 
          RULE_traversalGType = 182, RULE_traversalPredicate = 183, RULE_traversalTerminalMethod = 184, 
          RULE_traversalSackMethod = 185, RULE_traversalComparator = 186, 
          RULE_traversalFunction = 187, RULE_traversalBiFunction = 188, 
          RULE_traversalPredicate_eq = 189, RULE_traversalPredicate_neq = 190, 
          RULE_traversalPredicate_typeOf = 191, RULE_traversalPredicate_lt = 192, 
          RULE_traversalPredicate_lte = 193, RULE_traversalPredicate_gt = 194, 
          RULE_traversalPredicate_gte = 195, RULE_traversalPredicate_inside = 196, 
          RULE_traversalPredicate_outside = 197, RULE_traversalPredicate_between = 198, 
          RULE_traversalPredicate_within = 199, RULE_traversalPredicate_without = 200, 
          RULE_traversalPredicate_not = 201, RULE_traversalPredicate_containing = 202, 
          RULE_traversalPredicate_notContaining = 203, RULE_traversalPredicate_startingWith = 204, 
          RULE_traversalPredicate_notStartingWith = 205, RULE_traversalPredicate_endingWith = 206, 
          RULE_traversalPredicate_notEndingWith = 207, RULE_traversalPredicate_regex = 208, 
          RULE_traversalPredicate_notRegex = 209, RULE_traversalTerminalMethod_explain = 210, 
          RULE_traversalTerminalMethod_hasNext = 211, RULE_traversalTerminalMethod_iterate = 212, 
          RULE_traversalTerminalMethod_tryNext = 213, RULE_traversalTerminalMethod_next = 214, 
          RULE_traversalTerminalMethod_toList = 215, RULE_traversalTerminalMethod_toSet = 216, 
          RULE_traversalTerminalMethod_toBulkSet = 217, RULE_withOptionKeys = 218, 
          RULE_connectedComponentConstants = 219, RULE_pageRankConstants = 220, 
          RULE_peerPressureConstants = 221, RULE_shortestPathConstants = 222, 
          RULE_withOptionsValues = 223, RULE_ioOptionsKeys = 224, RULE_ioOptionsValues = 225, 
          RULE_connectedComponentConstants_component = 226, RULE_connectedComponentConstants_edges = 227, 
          RULE_connectedComponentConstants_propertyName = 228, RULE_pageRankConstants_edges = 229, 
          RULE_pageRankConstants_times = 230, RULE_pageRankConstants_propertyName = 231, 
          RULE_peerPressureConstants_edges = 232, RULE_peerPressureConstants_times = 233, 
          RULE_peerPressureConstants_propertyName = 234, RULE_shortestPathConstants_target = 235, 
          RULE_shortestPathConstants_edges = 236, RULE_shortestPathConstants_distance = 237, 
          RULE_shortestPathConstants_maxDistance = 238, RULE_shortestPathConstants_includeEdges = 239, 
          RULE_withOptionsConstants_tokens = 240, RULE_withOptionsConstants_none = 241, 
          RULE_withOptionsConstants_ids = 242, RULE_withOptionsConstants_labels = 243, 
          RULE_withOptionsConstants_keys = 244, RULE_withOptionsConstants_values = 245, 
          RULE_withOptionsConstants_all = 246, RULE_withOptionsConstants_indexer = 247, 
          RULE_withOptionsConstants_list = 248, RULE_withOptionsConstants_map = 249, 
          RULE_ioOptionsConstants_reader = 250, RULE_ioOptionsConstants_writer = 251, 
          RULE_ioOptionsConstants_gryo = 252, RULE_ioOptionsConstants_graphson = 253, 
          RULE_ioOptionsConstants_graphml = 254, RULE_connectedComponentStringConstant = 255, 
          RULE_pageRankStringConstant = 256, RULE_peerPressureStringConstant = 257, 
          RULE_shortestPathStringConstant = 258, RULE_withOptionsStringConstant = 259, 
          RULE_ioOptionsStringConstant = 260, RULE_booleanArgument = 261, 
          RULE_integerArgument = 262, RULE_stringArgument = 263, RULE_stringNullableArgument = 264, 
          RULE_stringNullableArgumentVarargs = 265, RULE_dateArgument = 266, 
          RULE_genericArgument = 267, RULE_genericArgumentVarargs = 268, 
          RULE_genericMapArgument = 269, RULE_genericMapNullableArgument = 270, 
          RULE_nullableGenericLiteralMap = 271, RULE_traversalStrategyVarargs = 272, 
          RULE_traversalStrategyExpr = 273, RULE_classTypeList = 274, RULE_classTypeExpr = 275, 
          RULE_nestedTraversalList = 276, RULE_nestedTraversalExpr = 277, 
          RULE_genericCollectionLiteral = 278, RULE_genericLiteralVarargs = 279, 
          RULE_genericLiteralExpr = 280, RULE_genericMapNullableLiteral = 281, 
          RULE_genericRangeLiteral = 282, RULE_genericSetLiteral = 283, 
          RULE_stringNullableLiteralVarargs = 284, RULE_genericLiteral = 285, 
          RULE_genericMapLiteral = 286, RULE_mapKey = 287, RULE_mapEntry = 288, 
          RULE_stringLiteral = 289, RULE_stringNullableLiteral = 290, RULE_integerLiteral = 291, 
          RULE_floatLiteral = 292, RULE_numericLiteral = 293, RULE_booleanLiteral = 294, 
          RULE_dateLiteral = 295, RULE_nullLiteral = 296, RULE_nanLiteral = 297, 
          RULE_infLiteral = 298, RULE_uuidLiteral = 299, RULE_characterLiteral = 300, 
          RULE_durationLiteral = 301, RULE_binaryLiteral = 302, RULE_nakedKey = 303, 
          RULE_classType = 304, RULE_variable = 305, RULE_keyword = 306;
class GremlinParser extends Parser {
  static final checkVersion = () => RuntimeMetaData.checkVersion('4.13.2', RuntimeMetaData.VERSION);
  static const int TOKEN_EOF = IntStream.EOF;

  static final List<DFA> _decisionToDFA = List.generate(
      _ATN.numberOfDecisions, (i) => DFA(_ATN.getDecisionState(i), i));
  static final PredictionContextCache _sharedContextCache = PredictionContextCache();
  static const int TOKEN_K_ADDALL = 1, TOKEN_K_ADDE = 2, TOKEN_K_ADDV = 3, 
                   TOKEN_K_AGGREGATE = 4, TOKEN_K_ALL = 5, TOKEN_K_AND = 6, 
                   TOKEN_K_ANY = 7, TOKEN_K_AS = 8, TOKEN_K_ASBOOL = 9, 
                   TOKEN_K_ASC = 10, TOKEN_K_ASDATE = 11, TOKEN_K_ASNUMBER = 12, 
                   TOKEN_K_ASSTRING = 13, TOKEN_K_ASSIGN = 14, TOKEN_K_BARRIER = 15, 
                   TOKEN_K_BARRIERU = 16, TOKEN_K_BEGIN = 17, TOKEN_K_BETWEEN = 18, 
                   TOKEN_K_BIGDECIMAL = 19, TOKEN_K_BIGDECIMALU = 20, TOKEN_K_BIGINT = 21, 
                   TOKEN_K_BIGINTU = 22, TOKEN_K_BINARY = 23, TOKEN_K_BINARYC = 24, 
                   TOKEN_K_BINARYU = 25, TOKEN_K_BOOLEAN = 26, TOKEN_K_BOOLEANU = 27, 
                   TOKEN_K_BOTH = 28, TOKEN_K_BOTHU = 29, TOKEN_K_BOTHE = 30, 
                   TOKEN_K_BOTHV = 31, TOKEN_K_BRANCH = 32, TOKEN_K_BY = 33, 
                   TOKEN_K_BYTE = 34, TOKEN_K_BYTEU = 35, TOKEN_K_CALL = 36, 
                   TOKEN_K_CAP = 37, TOKEN_K_CARDINALITY = 38, TOKEN_K_CHAR = 39, 
                   TOKEN_K_CHARU = 40, TOKEN_K_CHOOSE = 41, TOKEN_K_COALESCE = 42, 
                   TOKEN_K_COIN = 43, TOKEN_K_COLUMN = 44, TOKEN_K_COMBINE = 45, 
                   TOKEN_K_COMMIT = 46, TOKEN_K_COMPONENT = 47, TOKEN_K_CONCAT = 48, 
                   TOKEN_K_CONJOIN = 49, TOKEN_K_CONNECTEDCOMPONENT = 50, 
                   TOKEN_K_CONNECTEDCOMPONENTU = 51, TOKEN_K_CONSTANT = 52, 
                   TOKEN_K_CONTAINING = 53, TOKEN_K_COUNT = 54, TOKEN_K_CYCLICPATH = 55, 
                   TOKEN_K_DAY = 56, TOKEN_K_DATEADD = 57, TOKEN_K_DATEDIFF = 58, 
                   TOKEN_K_DATETIME = 59, TOKEN_K_DATETIMEC = 60, TOKEN_K_DATETIMEU = 61, 
                   TOKEN_K_DECR = 62, TOKEN_K_DEDUP = 63, TOKEN_K_DESC = 64, 
                   TOKEN_K_DIFFERENCE = 65, TOKEN_K_DISCARD = 66, TOKEN_K_DIRECTION = 67, 
                   TOKEN_K_DISJUNCT = 68, TOKEN_K_DISTANCE = 69, TOKEN_K_DIV = 70, 
                   TOKEN_K_DOUBLE = 71, TOKEN_K_DOUBLEU = 72, TOKEN_K_DROP = 73, 
                   TOKEN_K_DT = 74, TOKEN_K_DURATION = 75, TOKEN_K_DURATIONC = 76, 
                   TOKEN_K_DURATIONU = 77, TOKEN_K_E = 78, TOKEN_K_EDGE = 79, 
                   TOKEN_K_EDGEU = 80, TOKEN_K_EDGES = 81, TOKEN_K_ELEMENTMAP = 82, 
                   TOKEN_K_ELEMENT = 83, TOKEN_K_EMIT = 84, TOKEN_K_ENDINGWITH = 85, 
                   TOKEN_K_EQ = 86, TOKEN_K_EXPLAIN = 87, TOKEN_K_FAIL = 88, 
                   TOKEN_K_FALSE = 89, TOKEN_K_FILTER = 90, TOKEN_K_FIRST = 91, 
                   TOKEN_K_FLATMAP = 92, TOKEN_K_FLOAT = 93, TOKEN_K_FLOATU = 94, 
                   TOKEN_K_FOLD = 95, TOKEN_K_FORMAT = 96, TOKEN_K_FROM = 97, 
                   TOKEN_K_GLOBAL = 98, TOKEN_K_GT = 99, TOKEN_K_GTE = 100, 
                   TOKEN_K_GTYPE = 101, TOKEN_K_GRAPHML = 102, TOKEN_K_GRAPHSON = 103, 
                   TOKEN_K_GROUPCOUNT = 104, TOKEN_K_GROUP = 105, TOKEN_K_GRYO = 106, 
                   TOKEN_K_GRAPH = 107, TOKEN_K_GRAPHU = 108, TOKEN_K_HAS = 109, 
                   TOKEN_K_HASID = 110, TOKEN_K_HASKEY = 111, TOKEN_K_HASLABEL = 112, 
                   TOKEN_K_HASNEXT = 113, TOKEN_K_HASNOT = 114, TOKEN_K_HASVALUE = 115, 
                   TOKEN_K_HOUR = 116, TOKEN_K_ID = 117, TOKEN_K_IDENTITY = 118, 
                   TOKEN_K_IDS = 119, TOKEN_K_IN = 120, TOKEN_K_INU = 121, 
                   TOKEN_K_INE = 122, TOKEN_K_INCLUDEEDGES = 123, TOKEN_K_INCR = 124, 
                   TOKEN_K_INDEXER = 125, TOKEN_K_INDEX = 126, TOKEN_K_INFINITY = 127, 
                   TOKEN_K_INJECT = 128, TOKEN_K_INSIDE = 129, TOKEN_K_INT = 130, 
                   TOKEN_K_INTU = 131, TOKEN_K_INTERSECT = 132, TOKEN_K_INV = 133, 
                   TOKEN_K_IOU = 134, TOKEN_K_IO = 135, TOKEN_K_IS = 136, 
                   TOKEN_K_ITERATE = 137, TOKEN_K_KEY = 138, TOKEN_K_KEYS = 139, 
                   TOKEN_K_LABELS = 140, TOKEN_K_LABEL = 141, TOKEN_K_LAST = 142, 
                   TOKEN_K_LENGTH = 143, TOKEN_K_LIMIT = 144, TOKEN_K_LIST = 145, 
                   TOKEN_K_LISTU = 146, TOKEN_K_LOCAL = 147, TOKEN_K_LONG = 148, 
                   TOKEN_K_LONGU = 149, TOKEN_K_LOOPS = 150, TOKEN_K_LT = 151, 
                   TOKEN_K_LTE = 152, TOKEN_K_LTRIM = 153, TOKEN_K_MAP = 154, 
                   TOKEN_K_MAPU = 155, TOKEN_K_MATCH = 156, TOKEN_K_MATH = 157, 
                   TOKEN_K_MAX = 158, TOKEN_K_MAXDISTANCE = 159, TOKEN_K_MEAN = 160, 
                   TOKEN_K_MERGEU = 161, TOKEN_K_MERGE = 162, TOKEN_K_MERGEE = 163, 
                   TOKEN_K_MERGEV = 164, TOKEN_K_MIN = 165, TOKEN_K_MINUTE = 166, 
                   TOKEN_K_MINUS = 167, TOKEN_K_MIXED = 168, TOKEN_K_MULT = 169, 
                   TOKEN_K_N = 170, TOKEN_K_NAN = 171, TOKEN_K_NEGATE = 172, 
                   TOKEN_K_NEXT = 173, TOKEN_K_NONE = 174, TOKEN_K_NOTREGEX = 175, 
                   TOKEN_K_NOTCONTAINING = 176, TOKEN_K_NOTENDINGWITH = 177, 
                   TOKEN_K_NOTSTARTINGWITH = 178, TOKEN_K_NOT = 179, TOKEN_K_NEQ = 180, 
                   TOKEN_K_NEW = 181, TOKEN_K_NORMSACK = 182, TOKEN_K_NULL = 183, 
                   TOKEN_K_NULLU = 184, TOKEN_K_NUMBER = 185, TOKEN_K_NUMBERU = 186, 
                   TOKEN_K_ONCREATE = 187, TOKEN_K_ONMATCH = 188, TOKEN_K_OPERATOR = 189, 
                   TOKEN_K_OPTION = 190, TOKEN_K_OPTIONAL = 191, TOKEN_K_ORDERU = 192, 
                   TOKEN_K_ORDER = 193, TOKEN_K_OR = 194, TOKEN_K_OTHERV = 195, 
                   TOKEN_K_OUTU = 196, TOKEN_K_OUT = 197, TOKEN_K_OUTE = 198, 
                   TOKEN_K_OUTSIDE = 199, TOKEN_K_OUTV = 200, TOKEN_K_P = 201, 
                   TOKEN_K_PAGERANKU = 202, TOKEN_K_PAGERANK = 203, TOKEN_K_PATH = 204, 
                   TOKEN_K_PATHU = 205, TOKEN_K_PEERPRESSUREU = 206, TOKEN_K_PEERPRESSURE = 207, 
                   TOKEN_K_PICK = 208, TOKEN_K_POP = 209, TOKEN_K_PROFILE = 210, 
                   TOKEN_K_PROJECT = 211, TOKEN_K_PROPERTIES = 212, TOKEN_K_PROPERTYMAP = 213, 
                   TOKEN_K_PROPERTYNAME = 214, TOKEN_K_PROPERTY = 215, TOKEN_K_PROPERTYU = 216, 
                   TOKEN_K_PRODUCT = 217, TOKEN_K_RANGE = 218, TOKEN_K_READ = 219, 
                   TOKEN_K_READER = 220, TOKEN_K_REGEX = 221, TOKEN_K_REPLACE = 222, 
                   TOKEN_K_REPEAT = 223, TOKEN_K_REVERSE = 224, TOKEN_K_ROLLBACK = 225, 
                   TOKEN_K_RTRIM = 226, TOKEN_K_SACK = 227, TOKEN_K_SAMPLE = 228, 
                   TOKEN_K_SCOPE = 229, TOKEN_K_SECOND = 230, TOKEN_K_SELECT = 231, 
                   TOKEN_K_SET = 232, TOKEN_K_SETU = 233, TOKEN_K_SHORTESTPATHU = 234, 
                   TOKEN_K_SHORTESTPATH = 235, TOKEN_K_SHUFFLE = 236, TOKEN_K_SHORT = 237, 
                   TOKEN_K_SHORTU = 238, TOKEN_K_SIDEEFFECT = 239, TOKEN_K_SIMPLEPATH = 240, 
                   TOKEN_K_SINGLE = 241, TOKEN_K_SKIP = 242, TOKEN_K_SPLIT = 243, 
                   TOKEN_K_STARTINGWITH = 244, TOKEN_K_STRING = 245, TOKEN_K_STRINGU = 246, 
                   TOKEN_K_SUBGRAPH = 247, TOKEN_K_SUBSTRING = 248, TOKEN_K_SUM = 249, 
                   TOKEN_K_SUMLONG = 250, TOKEN_K_T = 251, TOKEN_K_TAIL = 252, 
                   TOKEN_K_TARGET = 253, TOKEN_K_TEXTP = 254, TOKEN_K_TIMELIMIT = 255, 
                   TOKEN_K_TIMES = 256, TOKEN_K_TO = 257, TOKEN_K_TOBULKSET = 258, 
                   TOKEN_K_TOKENS = 259, TOKEN_K_TOLIST = 260, TOKEN_K_TOLOWER = 261, 
                   TOKEN_K_TOSET = 262, TOKEN_K_TOSTRING = 263, TOKEN_K_TOUPPER = 264, 
                   TOKEN_K_TOE = 265, TOKEN_K_TOV = 266, TOKEN_K_TREE = 267, 
                   TOKEN_K_TREEU = 268, TOKEN_K_TRIM = 269, TOKEN_K_TRUE = 270, 
                   TOKEN_K_TRYNEXT = 271, TOKEN_K_TYPEOF = 272, TOKEN_K_TX = 273, 
                   TOKEN_K_UNFOLD = 274, TOKEN_K_UNION = 275, TOKEN_K_UNPRODUCTIVE = 276, 
                   TOKEN_K_UNTIL = 277, TOKEN_K_UUID = 278, TOKEN_K_UUIDL = 279, 
                   TOKEN_K_V = 280, TOKEN_K_VALUEMAP = 281, TOKEN_K_VALUES = 282, 
                   TOKEN_K_VALUE = 283, TOKEN_K_VERTEX = 284, TOKEN_K_VERTEXU = 285, 
                   TOKEN_K_VPROPERTY = 286, TOKEN_K_VPROPERTYU = 287, TOKEN_K_WHERE = 288, 
                   TOKEN_K_WITH = 289, TOKEN_K_WITHBULK = 290, TOKEN_K_WITHIN = 291, 
                   TOKEN_K_WITHOPTOPTIONS = 292, TOKEN_K_WITHOUT = 293, 
                   TOKEN_K_WITHOUTSTRATEGIES = 294, TOKEN_K_WITHPATH = 295, 
                   TOKEN_K_WITHSACK = 296, TOKEN_K_WITHSIDEEFFECT = 297, 
                   TOKEN_K_WITHSTRATEGIES = 298, TOKEN_K_WRITE = 299, TOKEN_K_WRITER = 300, 
                   TOKEN_IntegerLiteral = 301, TOKEN_FloatingPointLiteral = 302, 
                   TOKEN_SignedInfLiteral = 303, TOKEN_CharacterLiteral = 304, 
                   TOKEN_StringSuffixLiteral = 305, TOKEN_EmptyStringSuffixLiteral = 306, 
                   TOKEN_NonEmptyStringLiteral = 307, TOKEN_EmptyStringLiteral = 308, 
                   TOKEN_LPAREN = 309, TOKEN_RPAREN = 310, TOKEN_LBRACE = 311, 
                   TOKEN_RBRACE = 312, TOKEN_LBRACK = 313, TOKEN_RBRACK = 314, 
                   TOKEN_SEMI = 315, TOKEN_COMMA = 316, TOKEN_DOT = 317, 
                   TOKEN_COLON = 318, TOKEN_TRAVERSAL_ROOT = 319, TOKEN_ANON_TRAVERSAL_ROOT = 320, 
                   TOKEN_WS = 321, TOKEN_LINE_COMMENT = 322, TOKEN_Identifier = 323;

  @override
  final List<String> ruleNames = [
    'queryList', 'query', 'emptyQuery', 'traversalSource', 'transactionPart', 
    'rootTraversal', 'traversalSourceSelfMethod', 'traversalSourceSelfMethod_withBulk', 
    'traversalSourceSelfMethod_withPath', 'traversalSourceSelfMethod_withSack', 
    'traversalSourceSelfMethod_withSideEffect', 'traversalSourceSelfMethod_withStrategies', 
    'traversalSourceSelfMethod_withoutStrategies', 'traversalSourceSelfMethod_with', 
    'traversalSourceSpawnMethod', 'traversalSourceSpawnMethod_addE', 'traversalSourceSpawnMethod_addV', 
    'traversalSourceSpawnMethod_E', 'traversalSourceSpawnMethod_V', 'traversalSourceSpawnMethod_inject', 
    'traversalSourceSpawnMethod_io', 'traversalSourceSpawnMethod_mergeV', 
    'traversalSourceSpawnMethod_mergeE', 'traversalSourceSpawnMethod_call', 
    'traversalSourceSpawnMethod_union', 'chainedTraversal', 'nestedTraversal', 
    'terminatedTraversal', 'traversalMethod', 'traversalMethod_V', 'traversalMethod_E', 
    'traversalMethod_addE', 'traversalMethod_addV', 'traversalMethod_aggregate', 
    'traversalMethod_all', 'traversalMethod_and', 'traversalMethod_any', 
    'traversalMethod_as', 'traversalMethod_asBool', 'traversalMethod_asDate', 
    'traversalMethod_asNumber', 'traversalMethod_asString', 'traversalMethod_barrier', 
    'traversalMethod_both', 'traversalMethod_bothE', 'traversalMethod_bothV', 
    'traversalMethod_branch', 'traversalMethod_by', 'traversalMethod_call', 
    'traversalMethod_cap', 'traversalMethod_choose', 'traversalMethod_coalesce', 
    'traversalMethod_coin', 'traversalMethod_combine', 'traversalMethod_concat', 
    'traversalMethod_conjoin', 'traversalMethod_connectedComponent', 'traversalMethod_constant', 
    'traversalMethod_count', 'traversalMethod_cyclicPath', 'traversalMethod_dateAdd', 
    'traversalMethod_dateDiff', 'traversalMethod_dedup', 'traversalMethod_difference', 
    'traversalMethod_discard', 'traversalMethod_disjunct', 'traversalMethod_drop', 
    'traversalMethod_element', 'traversalMethod_elementMap', 'traversalMethod_emit', 
    'traversalMethod_fail', 'traversalMethod_filter', 'traversalMethod_flatMap', 
    'traversalMethod_fold', 'traversalMethod_format', 'traversalMethod_from', 
    'traversalMethod_group', 'traversalMethod_groupCount', 'traversalMethod_has', 
    'traversalMethod_hasId', 'traversalMethod_hasKey', 'traversalMethod_hasLabel', 
    'traversalMethod_hasNot', 'traversalMethod_hasValue', 'traversalMethod_id', 
    'traversalMethod_identity', 'traversalMethod_in', 'traversalMethod_inE', 
    'traversalMethod_intersect', 'traversalMethod_inV', 'traversalMethod_index', 
    'traversalMethod_inject', 'traversalMethod_is', 'traversalMethod_key', 
    'traversalMethod_label', 'traversalMethod_length', 'traversalMethod_limit', 
    'traversalMethod_local', 'traversalMethod_loops', 'traversalMethod_lTrim', 
    'traversalMethod_map', 'traversalMethod_match', 'traversalMethod_math', 
    'traversalMethod_max', 'traversalMethod_mean', 'traversalMethod_merge', 
    'traversalMethod_mergeV', 'traversalMethod_mergeE', 'traversalMethod_min', 
    'traversalMethod_none', 'traversalMethod_not', 'traversalMethod_option', 
    'traversalMethod_optional', 'traversalMethod_or', 'traversalMethod_order', 
    'traversalMethod_otherV', 'traversalMethod_out', 'traversalMethod_outE', 
    'traversalMethod_outV', 'traversalMethod_pageRank', 'traversalMethod_path', 
    'traversalMethod_peerPressure', 'traversalMethod_product', 'traversalMethod_profile', 
    'traversalMethod_project', 'traversalMethod_properties', 'traversalMethod_property', 
    'traversalMethod_propertyMap', 'traversalMethod_range', 'traversalMethod_read', 
    'traversalMethod_repeat', 'traversalMethod_replace', 'traversalMethod_reverse', 
    'traversalMethod_rTrim', 'traversalMethod_sack', 'traversalMethod_sample', 
    'traversalMethod_select', 'traversalMethod_shortestPath', 'traversalMethod_sideEffect', 
    'traversalMethod_simplePath', 'traversalMethod_skip', 'traversalMethod_split', 
    'traversalMethod_subgraph', 'traversalMethod_substring', 'traversalMethod_sum', 
    'traversalMethod_tail', 'traversalMethod_timeLimit', 'traversalMethod_times', 
    'traversalMethod_to', 'traversalMethod_toE', 'traversalMethod_toLower', 
    'traversalMethod_toUpper', 'traversalMethod_toV', 'traversalMethod_tree', 
    'traversalMethod_trim', 'traversalMethod_unfold', 'traversalMethod_union', 
    'traversalMethod_until', 'traversalMethod_value', 'traversalMethod_valueMap', 
    'traversalMethod_values', 'traversalMethod_where', 'traversalMethod_with', 
    'traversalMethod_write', 'traversalStrategy', 'configuration', 'traversalScope', 
    'traversalBarrier', 'traversalT', 'traversalTShort', 'traversalTLong', 
    'traversalMerge', 'traversalOrder', 'traversalDirection', 'traversalDirectionShort', 
    'traversalDirectionLong', 'traversalCardinality', 'traversalColumn', 
    'traversalPop', 'traversalOperator', 'traversalPick', 'traversalDT', 
    'traversalGType', 'traversalPredicate', 'traversalTerminalMethod', 'traversalSackMethod', 
    'traversalComparator', 'traversalFunction', 'traversalBiFunction', 'traversalPredicate_eq', 
    'traversalPredicate_neq', 'traversalPredicate_typeOf', 'traversalPredicate_lt', 
    'traversalPredicate_lte', 'traversalPredicate_gt', 'traversalPredicate_gte', 
    'traversalPredicate_inside', 'traversalPredicate_outside', 'traversalPredicate_between', 
    'traversalPredicate_within', 'traversalPredicate_without', 'traversalPredicate_not', 
    'traversalPredicate_containing', 'traversalPredicate_notContaining', 
    'traversalPredicate_startingWith', 'traversalPredicate_notStartingWith', 
    'traversalPredicate_endingWith', 'traversalPredicate_notEndingWith', 
    'traversalPredicate_regex', 'traversalPredicate_notRegex', 'traversalTerminalMethod_explain', 
    'traversalTerminalMethod_hasNext', 'traversalTerminalMethod_iterate', 
    'traversalTerminalMethod_tryNext', 'traversalTerminalMethod_next', 'traversalTerminalMethod_toList', 
    'traversalTerminalMethod_toSet', 'traversalTerminalMethod_toBulkSet', 
    'withOptionKeys', 'connectedComponentConstants', 'pageRankConstants', 
    'peerPressureConstants', 'shortestPathConstants', 'withOptionsValues', 
    'ioOptionsKeys', 'ioOptionsValues', 'connectedComponentConstants_component', 
    'connectedComponentConstants_edges', 'connectedComponentConstants_propertyName', 
    'pageRankConstants_edges', 'pageRankConstants_times', 'pageRankConstants_propertyName', 
    'peerPressureConstants_edges', 'peerPressureConstants_times', 'peerPressureConstants_propertyName', 
    'shortestPathConstants_target', 'shortestPathConstants_edges', 'shortestPathConstants_distance', 
    'shortestPathConstants_maxDistance', 'shortestPathConstants_includeEdges', 
    'withOptionsConstants_tokens', 'withOptionsConstants_none', 'withOptionsConstants_ids', 
    'withOptionsConstants_labels', 'withOptionsConstants_keys', 'withOptionsConstants_values', 
    'withOptionsConstants_all', 'withOptionsConstants_indexer', 'withOptionsConstants_list', 
    'withOptionsConstants_map', 'ioOptionsConstants_reader', 'ioOptionsConstants_writer', 
    'ioOptionsConstants_gryo', 'ioOptionsConstants_graphson', 'ioOptionsConstants_graphml', 
    'connectedComponentStringConstant', 'pageRankStringConstant', 'peerPressureStringConstant', 
    'shortestPathStringConstant', 'withOptionsStringConstant', 'ioOptionsStringConstant', 
    'booleanArgument', 'integerArgument', 'stringArgument', 'stringNullableArgument', 
    'stringNullableArgumentVarargs', 'dateArgument', 'genericArgument', 
    'genericArgumentVarargs', 'genericMapArgument', 'genericMapNullableArgument', 
    'nullableGenericLiteralMap', 'traversalStrategyVarargs', 'traversalStrategyExpr', 
    'classTypeList', 'classTypeExpr', 'nestedTraversalList', 'nestedTraversalExpr', 
    'genericCollectionLiteral', 'genericLiteralVarargs', 'genericLiteralExpr', 
    'genericMapNullableLiteral', 'genericRangeLiteral', 'genericSetLiteral', 
    'stringNullableLiteralVarargs', 'genericLiteral', 'genericMapLiteral', 
    'mapKey', 'mapEntry', 'stringLiteral', 'stringNullableLiteral', 'integerLiteral', 
    'floatLiteral', 'numericLiteral', 'booleanLiteral', 'dateLiteral', 'nullLiteral', 
    'nanLiteral', 'infLiteral', 'uuidLiteral', 'characterLiteral', 'durationLiteral', 
    'binaryLiteral', 'nakedKey', 'classType', 'variable', 'keyword'
  ];

  static final List<String?> _LITERAL_NAMES = [
      null, "'addAll'", "'addE'", "'addV'", "'aggregate'", "'all'", "'and'", 
      "'any'", "'as'", "'asBool'", "'asc'", "'asDate'", "'asNumber'", "'asString'", 
      "'assign'", "'barrier'", "'Barrier'", "'begin'", "'between'", "'bigDecimal'", 
      "'BIGDECIMAL'", "'bigInt'", "'BIGINT'", "'binary'", "'Binary'", "'BINARY'", 
      "'boolean'", "'BOOLEAN'", "'both'", "'BOTH'", "'bothE'", "'bothV'", 
      "'branch'", "'by'", "'byte'", "'BYTE'", "'call'", "'cap'", "'Cardinality'", 
      "'char'", "'CHAR'", "'choose'", "'coalesce'", "'coin'", "'Column'", 
      "'combine'", "'commit'", "'component'", "'concat'", "'conjoin'", "'connectedComponent'", 
      "'ConnectedComponent'", "'constant'", "'containing'", "'count'", "'cyclicPath'", 
      "'day'", "'dateAdd'", "'dateDiff'", "'datetime'", "'DateTime'", "'DATETIME'", 
      "'decr'", "'dedup'", "'desc'", "'difference'", "'discard'", "'Direction'", 
      "'disjunct'", "'distance'", "'div'", "'double'", "'DOUBLE'", "'drop'", 
      "'DT'", "'duration'", "'Duration'", "'DURATION'", "'E'", "'edge'", 
      "'EDGE'", "'edges'", "'elementMap'", "'element'", "'emit'", "'endingWith'", 
      "'eq'", "'explain'", "'fail'", "'false'", "'filter'", "'first'", "'flatMap'", 
      "'float'", "'FLOAT'", "'fold'", "'format'", "'from'", "'global'", 
      "'gt'", "'gte'", "'GType'", "'graphml'", "'graphson'", "'groupCount'", 
      "'group'", "'gryo'", "'graph'", "'GRAPH'", "'has'", "'hasId'", "'hasKey'", 
      "'hasLabel'", "'hasNext'", "'hasNot'", "'hasValue'", "'hour'", "'id'", 
      "'identity'", "'ids'", "'in'", "'IN'", "'inE'", "'includeEdges'", 
      "'incr'", "'indexer'", "'index'", "'Infinity'", "'inject'", "'inside'", 
      "'int'", "'INT'", "'intersect'", "'inV'", "'IO'", "'io'", "'is'", 
      "'iterate'", "'key'", "'keys'", "'labels'", "'label'", "'last'", "'length'", 
      "'limit'", "'list'", "'LIST'", "'local'", "'long'", "'LONG'", "'loops'", 
      "'lt'", "'lte'", "'lTrim'", "'map'", "'MAP'", "'match'", "'math'", 
      "'max'", "'maxDistance'", "'mean'", "'Merge'", "'merge'", "'mergeE'", 
      "'mergeV'", "'min'", "'minute'", "'minus'", "'mixed'", "'mult'", "'N'", 
      "'NaN'", "'negate'", "'next'", "'none'", "'notRegex'", "'notContaining'", 
      "'notEndingWith'", "'notStartingWith'", "'not'", "'neq'", "'new'", 
      "'normSack'", "'null'", "'NULL'", "'number'", "'NUMBER'", "'onCreate'", 
      "'onMatch'", "'Operator'", "'option'", "'optional'", "'Order'", "'order'", 
      "'or'", "'otherV'", "'OUT'", "'out'", "'outE'", "'outside'", "'outV'", 
      "'P'", "'PageRank'", "'pageRank'", "'path'", "'PATH'", "'PeerPressure'", 
      "'peerPressure'", "'Pick'", "'Pop'", "'profile'", "'project'", "'properties'", 
      "'propertyMap'", "'propertyName'", "'property'", "'PROPERTY'", "'product'", 
      "'range'", "'read'", "'reader'", "'regex'", "'replace'", "'repeat'", 
      "'reverse'", "'rollback'", "'rTrim'", "'sack'", "'sample'", "'Scope'", 
      "'second'", "'select'", "'set'", "'SET'", "'ShortestPath'", "'shortestPath'", 
      "'shuffle'", "'short'", "'SHORT'", "'sideEffect'", "'simplePath'", 
      "'single'", "'skip'", "'split'", "'startingWith'", "'string'", "'STRING'", 
      "'subgraph'", "'substring'", "'sum'", "'sumLong'", "'T'", "'tail'", 
      "'target'", "'TextP'", "'timeLimit'", "'times'", "'to'", "'toBulkSet'", 
      "'tokens'", "'toList'", "'toLower'", "'toSet'", "'toString'", "'toUpper'", 
      "'toE'", "'toV'", "'tree'", "'TREE'", "'trim'", "'true'", "'tryNext'", 
      "'typeOf'", "'tx'", "'unfold'", "'union'", "'unproductive'", "'until'", 
      "'UUID'", "'uuid'", "'V'", "'valueMap'", "'values'", "'value'", "'vertex'", 
      "'VERTEX'", "'vproperty'", "'VPROPERTY'", "'where'", "'with'", "'withBulk'", 
      "'within'", "'WithOptions'", "'without'", "'withoutStrategies'", "'withPath'", 
      "'withSack'", "'withSideEffect'", "'withStrategies'", "'write'", "'writer'", 
      null, null, null, null, null, null, null, null, "'('", "')'", "'{'", 
      "'}'", "'['", "']'", "';'", "','", "'.'", "':'", "'g'", "'__'"
  ];
  static final List<String?> _SYMBOLIC_NAMES = [
      null, "K_ADDALL", "K_ADDE", "K_ADDV", "K_AGGREGATE", "K_ALL", "K_AND", 
      "K_ANY", "K_AS", "K_ASBOOL", "K_ASC", "K_ASDATE", "K_ASNUMBER", "K_ASSTRING", 
      "K_ASSIGN", "K_BARRIER", "K_BARRIERU", "K_BEGIN", "K_BETWEEN", "K_BIGDECIMAL", 
      "K_BIGDECIMALU", "K_BIGINT", "K_BIGINTU", "K_BINARY", "K_BINARYC", 
      "K_BINARYU", "K_BOOLEAN", "K_BOOLEANU", "K_BOTH", "K_BOTHU", "K_BOTHE", 
      "K_BOTHV", "K_BRANCH", "K_BY", "K_BYTE", "K_BYTEU", "K_CALL", "K_CAP", 
      "K_CARDINALITY", "K_CHAR", "K_CHARU", "K_CHOOSE", "K_COALESCE", "K_COIN", 
      "K_COLUMN", "K_COMBINE", "K_COMMIT", "K_COMPONENT", "K_CONCAT", "K_CONJOIN", 
      "K_CONNECTEDCOMPONENT", "K_CONNECTEDCOMPONENTU", "K_CONSTANT", "K_CONTAINING", 
      "K_COUNT", "K_CYCLICPATH", "K_DAY", "K_DATEADD", "K_DATEDIFF", "K_DATETIME", 
      "K_DATETIMEC", "K_DATETIMEU", "K_DECR", "K_DEDUP", "K_DESC", "K_DIFFERENCE", 
      "K_DISCARD", "K_DIRECTION", "K_DISJUNCT", "K_DISTANCE", "K_DIV", "K_DOUBLE", 
      "K_DOUBLEU", "K_DROP", "K_DT", "K_DURATION", "K_DURATIONC", "K_DURATIONU", 
      "K_E", "K_EDGE", "K_EDGEU", "K_EDGES", "K_ELEMENTMAP", "K_ELEMENT", 
      "K_EMIT", "K_ENDINGWITH", "K_EQ", "K_EXPLAIN", "K_FAIL", "K_FALSE", 
      "K_FILTER", "K_FIRST", "K_FLATMAP", "K_FLOAT", "K_FLOATU", "K_FOLD", 
      "K_FORMAT", "K_FROM", "K_GLOBAL", "K_GT", "K_GTE", "K_GTYPE", "K_GRAPHML", 
      "K_GRAPHSON", "K_GROUPCOUNT", "K_GROUP", "K_GRYO", "K_GRAPH", "K_GRAPHU", 
      "K_HAS", "K_HASID", "K_HASKEY", "K_HASLABEL", "K_HASNEXT", "K_HASNOT", 
      "K_HASVALUE", "K_HOUR", "K_ID", "K_IDENTITY", "K_IDS", "K_IN", "K_INU", 
      "K_INE", "K_INCLUDEEDGES", "K_INCR", "K_INDEXER", "K_INDEX", "K_INFINITY", 
      "K_INJECT", "K_INSIDE", "K_INT", "K_INTU", "K_INTERSECT", "K_INV", 
      "K_IOU", "K_IO", "K_IS", "K_ITERATE", "K_KEY", "K_KEYS", "K_LABELS", 
      "K_LABEL", "K_LAST", "K_LENGTH", "K_LIMIT", "K_LIST", "K_LISTU", "K_LOCAL", 
      "K_LONG", "K_LONGU", "K_LOOPS", "K_LT", "K_LTE", "K_LTRIM", "K_MAP", 
      "K_MAPU", "K_MATCH", "K_MATH", "K_MAX", "K_MAXDISTANCE", "K_MEAN", 
      "K_MERGEU", "K_MERGE", "K_MERGEE", "K_MERGEV", "K_MIN", "K_MINUTE", 
      "K_MINUS", "K_MIXED", "K_MULT", "K_N", "K_NAN", "K_NEGATE", "K_NEXT", 
      "K_NONE", "K_NOTREGEX", "K_NOTCONTAINING", "K_NOTENDINGWITH", "K_NOTSTARTINGWITH", 
      "K_NOT", "K_NEQ", "K_NEW", "K_NORMSACK", "K_NULL", "K_NULLU", "K_NUMBER", 
      "K_NUMBERU", "K_ONCREATE", "K_ONMATCH", "K_OPERATOR", "K_OPTION", 
      "K_OPTIONAL", "K_ORDERU", "K_ORDER", "K_OR", "K_OTHERV", "K_OUTU", 
      "K_OUT", "K_OUTE", "K_OUTSIDE", "K_OUTV", "K_P", "K_PAGERANKU", "K_PAGERANK", 
      "K_PATH", "K_PATHU", "K_PEERPRESSUREU", "K_PEERPRESSURE", "K_PICK", 
      "K_POP", "K_PROFILE", "K_PROJECT", "K_PROPERTIES", "K_PROPERTYMAP", 
      "K_PROPERTYNAME", "K_PROPERTY", "K_PROPERTYU", "K_PRODUCT", "K_RANGE", 
      "K_READ", "K_READER", "K_REGEX", "K_REPLACE", "K_REPEAT", "K_REVERSE", 
      "K_ROLLBACK", "K_RTRIM", "K_SACK", "K_SAMPLE", "K_SCOPE", "K_SECOND", 
      "K_SELECT", "K_SET", "K_SETU", "K_SHORTESTPATHU", "K_SHORTESTPATH", 
      "K_SHUFFLE", "K_SHORT", "K_SHORTU", "K_SIDEEFFECT", "K_SIMPLEPATH", 
      "K_SINGLE", "K_SKIP", "K_SPLIT", "K_STARTINGWITH", "K_STRING", "K_STRINGU", 
      "K_SUBGRAPH", "K_SUBSTRING", "K_SUM", "K_SUMLONG", "K_T", "K_TAIL", 
      "K_TARGET", "K_TEXTP", "K_TIMELIMIT", "K_TIMES", "K_TO", "K_TOBULKSET", 
      "K_TOKENS", "K_TOLIST", "K_TOLOWER", "K_TOSET", "K_TOSTRING", "K_TOUPPER", 
      "K_TOE", "K_TOV", "K_TREE", "K_TREEU", "K_TRIM", "K_TRUE", "K_TRYNEXT", 
      "K_TYPEOF", "K_TX", "K_UNFOLD", "K_UNION", "K_UNPRODUCTIVE", "K_UNTIL", 
      "K_UUID", "K_UUIDL", "K_V", "K_VALUEMAP", "K_VALUES", "K_VALUE", "K_VERTEX", 
      "K_VERTEXU", "K_VPROPERTY", "K_VPROPERTYU", "K_WHERE", "K_WITH", "K_WITHBULK", 
      "K_WITHIN", "K_WITHOPTOPTIONS", "K_WITHOUT", "K_WITHOUTSTRATEGIES", 
      "K_WITHPATH", "K_WITHSACK", "K_WITHSIDEEFFECT", "K_WITHSTRATEGIES", 
      "K_WRITE", "K_WRITER", "IntegerLiteral", "FloatingPointLiteral", "SignedInfLiteral", 
      "CharacterLiteral", "StringSuffixLiteral", "EmptyStringSuffixLiteral", 
      "NonEmptyStringLiteral", "EmptyStringLiteral", "LPAREN", "RPAREN", 
      "LBRACE", "RBRACE", "LBRACK", "RBRACK", "SEMI", "COMMA", "DOT", "COLON", 
      "TRAVERSAL_ROOT", "ANON_TRAVERSAL_ROOT", "WS", "LINE_COMMENT", "Identifier"
  ];
  static final Vocabulary VOCABULARY = VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  @override
  Vocabulary get vocabulary {
    return VOCABULARY;
  }

  @override
  String get grammarFileName => 'Gremlin.g4';

  @override
  List<int> get serializedATN => _serializedATN;

  @override
  ATN getATN() {
   return _ATN;
  }

  GremlinParser(TokenStream input) : super(input) {
    interpreter = ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
  }

  QueryListContext queryList() {
    dynamic _localctx = QueryListContext(context, state);
    enterRule(_localctx, 0, RULE_queryList);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      state = 614;
      query(0);
      state = 621;
      errorHandler.sync(this);
      _alt = interpreter!.adaptivePredict(tokenStream, 1, context);
      while (_alt != 2 && _alt != ATN.INVALID_ALT_NUMBER) {
        if (_alt == 1) {
          state = 616;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
          if (_la == TOKEN_SEMI) {
            state = 615;
            match(TOKEN_SEMI);
          }

          state = 618;
          query(0); 
        }
        state = 623;
        errorHandler.sync(this);
        _alt = interpreter!.adaptivePredict(tokenStream, 1, context);
      }
      state = 625;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_SEMI) {
        state = 624;
        match(TOKEN_SEMI);
      }

      state = 627;
      match(TOKEN_EOF);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  QueryContext query([int _p = 0]) {
    final _parentctx = context;
    final _parentState = state;
    dynamic _localctx = QueryContext(context, _parentState);
    var _prevctx = _localctx;
    var _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      state = 641;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 3, context)) {
      case 1:
        state = 630;
        traversalSource(0);
        break;
      case 2:
        state = 631;
        traversalSource(0);
        state = 632;
        match(TOKEN_DOT);
        state = 633;
        transactionPart();
        break;
      case 3:
        state = 635;
        rootTraversal();
        break;
      case 4:
        state = 636;
        rootTraversal();
        state = 637;
        match(TOKEN_DOT);
        state = 638;
        traversalTerminalMethod();
        break;
      case 5:
        state = 640;
        emptyQuery();
        break;
      }
      context!.stop = tokenStream.LT(-1);
      state = 650;
      errorHandler.sync(this);
      _alt = interpreter!.adaptivePredict(tokenStream, 4, context);
      while (_alt != 2 && _alt != ATN.INVALID_ALT_NUMBER) {
        if (_alt == 1) {
          if (parseListeners != null) triggerExitRuleEvent();
          _prevctx = _localctx;
          _localctx = QueryContext(_parentctx, _parentState);
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          state = 643;
          if (!(precpred(context, 2))) {
            throw FailedPredicateException(this, "precpred(context, 2)");
          }
          state = 644;
          match(TOKEN_DOT);
          state = 645;
          match(TOKEN_K_TOSTRING);
          state = 646;
          match(TOKEN_LPAREN);
          state = 647;
          match(TOKEN_RPAREN); 
        }
        state = 652;
        errorHandler.sync(this);
        _alt = interpreter!.adaptivePredict(tokenStream, 4, context);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  EmptyQueryContext emptyQuery() {
    dynamic _localctx = EmptyQueryContext(context, state);
    enterRule(_localctx, 4, RULE_emptyQuery);
    try {
      enterOuterAlt(_localctx, 1);
      state = 653;
      match(TOKEN_EmptyStringLiteral);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceContext traversalSource([int _p = 0]) {
    final _parentctx = context;
    final _parentState = state;
    dynamic _localctx = TraversalSourceContext(context, _parentState);
    var _prevctx = _localctx;
    var _startState = 6;
    enterRecursionRule(_localctx, 6, RULE_traversalSource, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      state = 660;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 5, context)) {
      case 1:
        state = 656;
        match(TOKEN_TRAVERSAL_ROOT);
        break;
      case 2:
        state = 657;
        match(TOKEN_TRAVERSAL_ROOT);
        state = 658;
        match(TOKEN_DOT);
        state = 659;
        traversalSourceSelfMethod();
        break;
      }
      context!.stop = tokenStream.LT(-1);
      state = 667;
      errorHandler.sync(this);
      _alt = interpreter!.adaptivePredict(tokenStream, 6, context);
      while (_alt != 2 && _alt != ATN.INVALID_ALT_NUMBER) {
        if (_alt == 1) {
          if (parseListeners != null) triggerExitRuleEvent();
          _prevctx = _localctx;
          _localctx = TraversalSourceContext(_parentctx, _parentState);
          pushNewRecursionContext(_localctx, _startState, RULE_traversalSource);
          state = 662;
          if (!(precpred(context, 1))) {
            throw FailedPredicateException(this, "precpred(context, 1)");
          }
          state = 663;
          match(TOKEN_DOT);
          state = 664;
          traversalSourceSelfMethod(); 
        }
        state = 669;
        errorHandler.sync(this);
        _alt = interpreter!.adaptivePredict(tokenStream, 6, context);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  TransactionPartContext transactionPart() {
    dynamic _localctx = TransactionPartContext(context, state);
    enterRule(_localctx, 8, RULE_transactionPart);
    try {
      state = 691;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 7, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 670;
        match(TOKEN_K_TX);
        state = 671;
        match(TOKEN_LPAREN);
        state = 672;
        match(TOKEN_RPAREN);
        state = 673;
        match(TOKEN_DOT);
        state = 674;
        match(TOKEN_K_BEGIN);
        state = 675;
        match(TOKEN_LPAREN);
        state = 676;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 677;
        match(TOKEN_K_TX);
        state = 678;
        match(TOKEN_LPAREN);
        state = 679;
        match(TOKEN_RPAREN);
        state = 680;
        match(TOKEN_DOT);
        state = 681;
        match(TOKEN_K_COMMIT);
        state = 682;
        match(TOKEN_LPAREN);
        state = 683;
        match(TOKEN_RPAREN);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 684;
        match(TOKEN_K_TX);
        state = 685;
        match(TOKEN_LPAREN);
        state = 686;
        match(TOKEN_RPAREN);
        state = 687;
        match(TOKEN_DOT);
        state = 688;
        match(TOKEN_K_ROLLBACK);
        state = 689;
        match(TOKEN_LPAREN);
        state = 690;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  RootTraversalContext rootTraversal() {
    dynamic _localctx = RootTraversalContext(context, state);
    enterRule(_localctx, 10, RULE_rootTraversal);
    try {
      state = 703;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 8, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 693;
        traversalSource(0);
        state = 694;
        match(TOKEN_DOT);
        state = 695;
        traversalSourceSpawnMethod();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 697;
        traversalSource(0);
        state = 698;
        match(TOKEN_DOT);
        state = 699;
        traversalSourceSpawnMethod();
        state = 700;
        match(TOKEN_DOT);
        state = 701;
        chainedTraversal(0);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethodContext traversalSourceSelfMethod() {
    dynamic _localctx = TraversalSourceSelfMethodContext(context, state);
    enterRule(_localctx, 12, RULE_traversalSourceSelfMethod);
    try {
      state = 712;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_WITHBULK:
        enterOuterAlt(_localctx, 1);
        state = 705;
        traversalSourceSelfMethod_withBulk();
        break;
      case TOKEN_K_WITHPATH:
        enterOuterAlt(_localctx, 2);
        state = 706;
        traversalSourceSelfMethod_withPath();
        break;
      case TOKEN_K_WITHSACK:
        enterOuterAlt(_localctx, 3);
        state = 707;
        traversalSourceSelfMethod_withSack();
        break;
      case TOKEN_K_WITHSIDEEFFECT:
        enterOuterAlt(_localctx, 4);
        state = 708;
        traversalSourceSelfMethod_withSideEffect();
        break;
      case TOKEN_K_WITHSTRATEGIES:
        enterOuterAlt(_localctx, 5);
        state = 709;
        traversalSourceSelfMethod_withStrategies();
        break;
      case TOKEN_K_WITHOUTSTRATEGIES:
        enterOuterAlt(_localctx, 6);
        state = 710;
        traversalSourceSelfMethod_withoutStrategies();
        break;
      case TOKEN_K_WITH:
        enterOuterAlt(_localctx, 7);
        state = 711;
        traversalSourceSelfMethod_with();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withBulkContext traversalSourceSelfMethod_withBulk() {
    dynamic _localctx = TraversalSourceSelfMethod_withBulkContext(context, state);
    enterRule(_localctx, 14, RULE_traversalSourceSelfMethod_withBulk);
    try {
      enterOuterAlt(_localctx, 1);
      state = 714;
      match(TOKEN_K_WITHBULK);
      state = 715;
      match(TOKEN_LPAREN);
      state = 716;
      booleanLiteral();
      state = 717;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withPathContext traversalSourceSelfMethod_withPath() {
    dynamic _localctx = TraversalSourceSelfMethod_withPathContext(context, state);
    enterRule(_localctx, 16, RULE_traversalSourceSelfMethod_withPath);
    try {
      enterOuterAlt(_localctx, 1);
      state = 719;
      match(TOKEN_K_WITHPATH);
      state = 720;
      match(TOKEN_LPAREN);
      state = 721;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withSackContext traversalSourceSelfMethod_withSack() {
    dynamic _localctx = TraversalSourceSelfMethod_withSackContext(context, state);
    enterRule(_localctx, 18, RULE_traversalSourceSelfMethod_withSack);
    try {
      state = 735;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 10, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 723;
        match(TOKEN_K_WITHSACK);
        state = 724;
        match(TOKEN_LPAREN);
        state = 725;
        genericLiteral();
        state = 726;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 728;
        match(TOKEN_K_WITHSACK);
        state = 729;
        match(TOKEN_LPAREN);
        state = 730;
        genericLiteral();
        state = 731;
        match(TOKEN_COMMA);
        state = 732;
        traversalBiFunction();
        state = 733;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withSideEffectContext traversalSourceSelfMethod_withSideEffect() {
    dynamic _localctx = TraversalSourceSelfMethod_withSideEffectContext(context, state);
    enterRule(_localctx, 20, RULE_traversalSourceSelfMethod_withSideEffect);
    try {
      state = 753;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 11, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 737;
        match(TOKEN_K_WITHSIDEEFFECT);
        state = 738;
        match(TOKEN_LPAREN);
        state = 739;
        stringLiteral();
        state = 740;
        match(TOKEN_COMMA);
        state = 741;
        genericLiteral();
        state = 742;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 744;
        match(TOKEN_K_WITHSIDEEFFECT);
        state = 745;
        match(TOKEN_LPAREN);
        state = 746;
        stringLiteral();
        state = 747;
        match(TOKEN_COMMA);
        state = 748;
        genericLiteral();
        state = 749;
        match(TOKEN_COMMA);
        state = 750;
        traversalBiFunction();
        state = 751;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withStrategiesContext traversalSourceSelfMethod_withStrategies() {
    dynamic _localctx = TraversalSourceSelfMethod_withStrategiesContext(context, state);
    enterRule(_localctx, 22, RULE_traversalSourceSelfMethod_withStrategies);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 755;
      match(TOKEN_K_WITHSTRATEGIES);
      state = 756;
      match(TOKEN_LPAREN);
      state = 757;
      traversalStrategy();
      state = 760;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 758;
        match(TOKEN_COMMA);
        state = 759;
        traversalStrategyVarargs();
      }

      state = 762;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withoutStrategiesContext traversalSourceSelfMethod_withoutStrategies() {
    dynamic _localctx = TraversalSourceSelfMethod_withoutStrategiesContext(context, state);
    enterRule(_localctx, 24, RULE_traversalSourceSelfMethod_withoutStrategies);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 764;
      match(TOKEN_K_WITHOUTSTRATEGIES);
      state = 765;
      match(TOKEN_LPAREN);
      state = 766;
      classType();
      state = 769;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 767;
        match(TOKEN_COMMA);
        state = 768;
        classTypeList();
      }

      state = 771;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSelfMethod_withContext traversalSourceSelfMethod_with() {
    dynamic _localctx = TraversalSourceSelfMethod_withContext(context, state);
    enterRule(_localctx, 26, RULE_traversalSourceSelfMethod_with);
    try {
      state = 785;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 14, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 773;
        match(TOKEN_K_WITH);
        state = 774;
        match(TOKEN_LPAREN);
        state = 775;
        stringLiteral();
        state = 776;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 778;
        match(TOKEN_K_WITH);
        state = 779;
        match(TOKEN_LPAREN);
        state = 780;
        stringLiteral();
        state = 781;
        match(TOKEN_COMMA);
        state = 782;
        genericLiteral();
        state = 783;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethodContext traversalSourceSpawnMethod() {
    dynamic _localctx = TraversalSourceSpawnMethodContext(context, state);
    enterRule(_localctx, 28, RULE_traversalSourceSpawnMethod);
    try {
      state = 797;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ADDE:
        enterOuterAlt(_localctx, 1);
        state = 787;
        traversalSourceSpawnMethod_addE();
        break;
      case TOKEN_K_ADDV:
        enterOuterAlt(_localctx, 2);
        state = 788;
        traversalSourceSpawnMethod_addV();
        break;
      case TOKEN_K_E:
        enterOuterAlt(_localctx, 3);
        state = 789;
        traversalSourceSpawnMethod_E();
        break;
      case TOKEN_K_V:
        enterOuterAlt(_localctx, 4);
        state = 790;
        traversalSourceSpawnMethod_V();
        break;
      case TOKEN_K_MERGEE:
        enterOuterAlt(_localctx, 5);
        state = 791;
        traversalSourceSpawnMethod_mergeE();
        break;
      case TOKEN_K_MERGEV:
        enterOuterAlt(_localctx, 6);
        state = 792;
        traversalSourceSpawnMethod_mergeV();
        break;
      case TOKEN_K_INJECT:
        enterOuterAlt(_localctx, 7);
        state = 793;
        traversalSourceSpawnMethod_inject();
        break;
      case TOKEN_K_IO:
        enterOuterAlt(_localctx, 8);
        state = 794;
        traversalSourceSpawnMethod_io();
        break;
      case TOKEN_K_CALL:
        enterOuterAlt(_localctx, 9);
        state = 795;
        traversalSourceSpawnMethod_call();
        break;
      case TOKEN_K_UNION:
        enterOuterAlt(_localctx, 10);
        state = 796;
        traversalSourceSpawnMethod_union();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_addEContext traversalSourceSpawnMethod_addE() {
    dynamic _localctx = TraversalSourceSpawnMethod_addEContext(context, state);
    enterRule(_localctx, 30, RULE_traversalSourceSpawnMethod_addE);
    try {
      state = 809;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 16, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 799;
        match(TOKEN_K_ADDE);
        state = 800;
        match(TOKEN_LPAREN);
        state = 801;
        stringArgument();
        state = 802;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 804;
        match(TOKEN_K_ADDE);
        state = 805;
        match(TOKEN_LPAREN);
        state = 806;
        nestedTraversal();
        state = 807;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_addVContext traversalSourceSpawnMethod_addV() {
    dynamic _localctx = TraversalSourceSpawnMethod_addVContext(context, state);
    enterRule(_localctx, 32, RULE_traversalSourceSpawnMethod_addV);
    try {
      state = 824;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 17, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 811;
        match(TOKEN_K_ADDV);
        state = 812;
        match(TOKEN_LPAREN);
        state = 813;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 814;
        match(TOKEN_K_ADDV);
        state = 815;
        match(TOKEN_LPAREN);
        state = 816;
        stringArgument();
        state = 817;
        match(TOKEN_RPAREN);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 819;
        match(TOKEN_K_ADDV);
        state = 820;
        match(TOKEN_LPAREN);
        state = 821;
        nestedTraversal();
        state = 822;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_EContext traversalSourceSpawnMethod_E() {
    dynamic _localctx = TraversalSourceSpawnMethod_EContext(context, state);
    enterRule(_localctx, 34, RULE_traversalSourceSpawnMethod_E);
    try {
      enterOuterAlt(_localctx, 1);
      state = 826;
      match(TOKEN_K_E);
      state = 827;
      match(TOKEN_LPAREN);
      state = 828;
      genericArgumentVarargs();
      state = 829;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_VContext traversalSourceSpawnMethod_V() {
    dynamic _localctx = TraversalSourceSpawnMethod_VContext(context, state);
    enterRule(_localctx, 36, RULE_traversalSourceSpawnMethod_V);
    try {
      enterOuterAlt(_localctx, 1);
      state = 831;
      match(TOKEN_K_V);
      state = 832;
      match(TOKEN_LPAREN);
      state = 833;
      genericArgumentVarargs();
      state = 834;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_injectContext traversalSourceSpawnMethod_inject() {
    dynamic _localctx = TraversalSourceSpawnMethod_injectContext(context, state);
    enterRule(_localctx, 38, RULE_traversalSourceSpawnMethod_inject);
    try {
      enterOuterAlt(_localctx, 1);
      state = 836;
      match(TOKEN_K_INJECT);
      state = 837;
      match(TOKEN_LPAREN);
      state = 838;
      genericLiteralVarargs();
      state = 839;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_ioContext traversalSourceSpawnMethod_io() {
    dynamic _localctx = TraversalSourceSpawnMethod_ioContext(context, state);
    enterRule(_localctx, 40, RULE_traversalSourceSpawnMethod_io);
    try {
      enterOuterAlt(_localctx, 1);
      state = 841;
      match(TOKEN_K_IO);
      state = 842;
      match(TOKEN_LPAREN);
      state = 843;
      stringLiteral();
      state = 844;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_mergeVContext traversalSourceSpawnMethod_mergeV() {
    dynamic _localctx = TraversalSourceSpawnMethod_mergeVContext(context, state);
    enterRule(_localctx, 42, RULE_traversalSourceSpawnMethod_mergeV);
    try {
      state = 856;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 18, context)) {
      case 1:
        _localctx = TraversalSourceSpawnMethod_mergeV_MapContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 846;
        match(TOKEN_K_MERGEV);
        state = 847;
        match(TOKEN_LPAREN);
        state = 848;
        genericMapNullableArgument();
        state = 849;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalSourceSpawnMethod_mergeV_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 851;
        match(TOKEN_K_MERGEV);
        state = 852;
        match(TOKEN_LPAREN);
        state = 853;
        nestedTraversal();
        state = 854;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_mergeEContext traversalSourceSpawnMethod_mergeE() {
    dynamic _localctx = TraversalSourceSpawnMethod_mergeEContext(context, state);
    enterRule(_localctx, 44, RULE_traversalSourceSpawnMethod_mergeE);
    try {
      state = 868;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 19, context)) {
      case 1:
        _localctx = TraversalSourceSpawnMethod_mergeE_MapContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 858;
        match(TOKEN_K_MERGEE);
        state = 859;
        match(TOKEN_LPAREN);
        state = 860;
        genericMapNullableArgument();
        state = 861;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalSourceSpawnMethod_mergeE_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 863;
        match(TOKEN_K_MERGEE);
        state = 864;
        match(TOKEN_LPAREN);
        state = 865;
        nestedTraversal();
        state = 866;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_callContext traversalSourceSpawnMethod_call() {
    dynamic _localctx = TraversalSourceSpawnMethod_callContext(context, state);
    enterRule(_localctx, 46, RULE_traversalSourceSpawnMethod_call);
    try {
      state = 901;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 20, context)) {
      case 1:
        _localctx = TraversalSourceSpawnMethod_call_emptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 870;
        match(TOKEN_K_CALL);
        state = 871;
        match(TOKEN_LPAREN);
        state = 872;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalSourceSpawnMethod_call_stringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 873;
        match(TOKEN_K_CALL);
        state = 874;
        match(TOKEN_LPAREN);
        state = 875;
        stringLiteral();
        state = 876;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalSourceSpawnMethod_call_string_mapContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 878;
        match(TOKEN_K_CALL);
        state = 879;
        match(TOKEN_LPAREN);
        state = 880;
        stringLiteral();
        state = 881;
        match(TOKEN_COMMA);
        state = 882;
        genericMapArgument();
        state = 883;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalSourceSpawnMethod_call_string_traversalContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 885;
        match(TOKEN_K_CALL);
        state = 886;
        match(TOKEN_LPAREN);
        state = 887;
        stringLiteral();
        state = 888;
        match(TOKEN_COMMA);
        state = 889;
        nestedTraversal();
        state = 890;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalSourceSpawnMethod_call_string_map_traversalContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 892;
        match(TOKEN_K_CALL);
        state = 893;
        match(TOKEN_LPAREN);
        state = 894;
        stringLiteral();
        state = 895;
        match(TOKEN_COMMA);
        state = 896;
        genericMapArgument();
        state = 897;
        match(TOKEN_COMMA);
        state = 898;
        nestedTraversal();
        state = 899;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSourceSpawnMethod_unionContext traversalSourceSpawnMethod_union() {
    dynamic _localctx = TraversalSourceSpawnMethod_unionContext(context, state);
    enterRule(_localctx, 48, RULE_traversalSourceSpawnMethod_union);
    try {
      enterOuterAlt(_localctx, 1);
      state = 903;
      match(TOKEN_K_UNION);
      state = 904;
      match(TOKEN_LPAREN);
      state = 905;
      nestedTraversalList();
      state = 906;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ChainedTraversalContext chainedTraversal([int _p = 0]) {
    final _parentctx = context;
    final _parentState = state;
    dynamic _localctx = ChainedTraversalContext(context, _parentState);
    var _prevctx = _localctx;
    var _startState = 50;
    enterRecursionRule(_localctx, 50, RULE_chainedTraversal, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      state = 909;
      traversalMethod();
      context!.stop = tokenStream.LT(-1);
      state = 916;
      errorHandler.sync(this);
      _alt = interpreter!.adaptivePredict(tokenStream, 21, context);
      while (_alt != 2 && _alt != ATN.INVALID_ALT_NUMBER) {
        if (_alt == 1) {
          if (parseListeners != null) triggerExitRuleEvent();
          _prevctx = _localctx;
          _localctx = ChainedTraversalContext(_parentctx, _parentState);
          pushNewRecursionContext(_localctx, _startState, RULE_chainedTraversal);
          state = 911;
          if (!(precpred(context, 1))) {
            throw FailedPredicateException(this, "precpred(context, 1)");
          }
          state = 912;
          match(TOKEN_DOT);
          state = 913;
          traversalMethod(); 
        }
        state = 918;
        errorHandler.sync(this);
        _alt = interpreter!.adaptivePredict(tokenStream, 21, context);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  NestedTraversalContext nestedTraversal() {
    dynamic _localctx = NestedTraversalContext(context, state);
    enterRule(_localctx, 52, RULE_nestedTraversal);
    try {
      state = 923;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ADDE:
      case TOKEN_K_ADDV:
      case TOKEN_K_AGGREGATE:
      case TOKEN_K_ALL:
      case TOKEN_K_AND:
      case TOKEN_K_ANY:
      case TOKEN_K_AS:
      case TOKEN_K_ASBOOL:
      case TOKEN_K_ASDATE:
      case TOKEN_K_ASNUMBER:
      case TOKEN_K_ASSTRING:
      case TOKEN_K_BARRIER:
      case TOKEN_K_BOTH:
      case TOKEN_K_BOTHE:
      case TOKEN_K_BOTHV:
      case TOKEN_K_BRANCH:
      case TOKEN_K_BY:
      case TOKEN_K_CALL:
      case TOKEN_K_CAP:
      case TOKEN_K_CHOOSE:
      case TOKEN_K_COALESCE:
      case TOKEN_K_COIN:
      case TOKEN_K_COMBINE:
      case TOKEN_K_CONCAT:
      case TOKEN_K_CONJOIN:
      case TOKEN_K_CONNECTEDCOMPONENT:
      case TOKEN_K_CONSTANT:
      case TOKEN_K_COUNT:
      case TOKEN_K_CYCLICPATH:
      case TOKEN_K_DATEADD:
      case TOKEN_K_DATEDIFF:
      case TOKEN_K_DEDUP:
      case TOKEN_K_DIFFERENCE:
      case TOKEN_K_DISCARD:
      case TOKEN_K_DISJUNCT:
      case TOKEN_K_DROP:
      case TOKEN_K_E:
      case TOKEN_K_ELEMENTMAP:
      case TOKEN_K_ELEMENT:
      case TOKEN_K_EMIT:
      case TOKEN_K_FAIL:
      case TOKEN_K_FILTER:
      case TOKEN_K_FLATMAP:
      case TOKEN_K_FOLD:
      case TOKEN_K_FORMAT:
      case TOKEN_K_FROM:
      case TOKEN_K_GROUPCOUNT:
      case TOKEN_K_GROUP:
      case TOKEN_K_HAS:
      case TOKEN_K_HASID:
      case TOKEN_K_HASKEY:
      case TOKEN_K_HASLABEL:
      case TOKEN_K_HASNOT:
      case TOKEN_K_HASVALUE:
      case TOKEN_K_ID:
      case TOKEN_K_IDENTITY:
      case TOKEN_K_IN:
      case TOKEN_K_INE:
      case TOKEN_K_INDEX:
      case TOKEN_K_INJECT:
      case TOKEN_K_INTERSECT:
      case TOKEN_K_INV:
      case TOKEN_K_IS:
      case TOKEN_K_KEY:
      case TOKEN_K_LABEL:
      case TOKEN_K_LENGTH:
      case TOKEN_K_LIMIT:
      case TOKEN_K_LOCAL:
      case TOKEN_K_LOOPS:
      case TOKEN_K_LTRIM:
      case TOKEN_K_MAP:
      case TOKEN_K_MATCH:
      case TOKEN_K_MATH:
      case TOKEN_K_MAX:
      case TOKEN_K_MEAN:
      case TOKEN_K_MERGE:
      case TOKEN_K_MERGEE:
      case TOKEN_K_MERGEV:
      case TOKEN_K_MIN:
      case TOKEN_K_NONE:
      case TOKEN_K_NOT:
      case TOKEN_K_OPTION:
      case TOKEN_K_OPTIONAL:
      case TOKEN_K_ORDER:
      case TOKEN_K_OR:
      case TOKEN_K_OTHERV:
      case TOKEN_K_OUT:
      case TOKEN_K_OUTE:
      case TOKEN_K_OUTV:
      case TOKEN_K_PAGERANK:
      case TOKEN_K_PATH:
      case TOKEN_K_PEERPRESSURE:
      case TOKEN_K_PROFILE:
      case TOKEN_K_PROJECT:
      case TOKEN_K_PROPERTIES:
      case TOKEN_K_PROPERTYMAP:
      case TOKEN_K_PROPERTY:
      case TOKEN_K_PRODUCT:
      case TOKEN_K_RANGE:
      case TOKEN_K_READ:
      case TOKEN_K_REPLACE:
      case TOKEN_K_REPEAT:
      case TOKEN_K_REVERSE:
      case TOKEN_K_RTRIM:
      case TOKEN_K_SACK:
      case TOKEN_K_SAMPLE:
      case TOKEN_K_SELECT:
      case TOKEN_K_SHORTESTPATH:
      case TOKEN_K_SIDEEFFECT:
      case TOKEN_K_SIMPLEPATH:
      case TOKEN_K_SKIP:
      case TOKEN_K_SPLIT:
      case TOKEN_K_SUBGRAPH:
      case TOKEN_K_SUBSTRING:
      case TOKEN_K_SUM:
      case TOKEN_K_TAIL:
      case TOKEN_K_TIMELIMIT:
      case TOKEN_K_TIMES:
      case TOKEN_K_TO:
      case TOKEN_K_TOLOWER:
      case TOKEN_K_TOUPPER:
      case TOKEN_K_TOE:
      case TOKEN_K_TOV:
      case TOKEN_K_TREE:
      case TOKEN_K_TRIM:
      case TOKEN_K_UNFOLD:
      case TOKEN_K_UNION:
      case TOKEN_K_UNTIL:
      case TOKEN_K_V:
      case TOKEN_K_VALUEMAP:
      case TOKEN_K_VALUES:
      case TOKEN_K_VALUE:
      case TOKEN_K_WHERE:
      case TOKEN_K_WITH:
      case TOKEN_K_WRITE:
        enterOuterAlt(_localctx, 1);
        state = 919;
        chainedTraversal(0);
        break;
      case TOKEN_ANON_TRAVERSAL_ROOT:
        enterOuterAlt(_localctx, 2);
        state = 920;
        match(TOKEN_ANON_TRAVERSAL_ROOT);
        state = 921;
        match(TOKEN_DOT);
        state = 922;
        chainedTraversal(0);
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TerminatedTraversalContext terminatedTraversal() {
    dynamic _localctx = TerminatedTraversalContext(context, state);
    enterRule(_localctx, 54, RULE_terminatedTraversal);
    try {
      enterOuterAlt(_localctx, 1);
      state = 925;
      rootTraversal();
      state = 926;
      match(TOKEN_DOT);
      state = 927;
      traversalTerminalMethod();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethodContext traversalMethod() {
    dynamic _localctx = TraversalMethodContext(context, state);
    enterRule(_localctx, 56, RULE_traversalMethod);
    try {
      state = 1064;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_V:
        enterOuterAlt(_localctx, 1);
        state = 929;
        traversalMethod_V();
        break;
      case TOKEN_K_E:
        enterOuterAlt(_localctx, 2);
        state = 930;
        traversalMethod_E();
        break;
      case TOKEN_K_ADDE:
        enterOuterAlt(_localctx, 3);
        state = 931;
        traversalMethod_addE();
        break;
      case TOKEN_K_ADDV:
        enterOuterAlt(_localctx, 4);
        state = 932;
        traversalMethod_addV();
        break;
      case TOKEN_K_MERGEE:
        enterOuterAlt(_localctx, 5);
        state = 933;
        traversalMethod_mergeE();
        break;
      case TOKEN_K_MERGEV:
        enterOuterAlt(_localctx, 6);
        state = 934;
        traversalMethod_mergeV();
        break;
      case TOKEN_K_AGGREGATE:
        enterOuterAlt(_localctx, 7);
        state = 935;
        traversalMethod_aggregate();
        break;
      case TOKEN_K_ALL:
        enterOuterAlt(_localctx, 8);
        state = 936;
        traversalMethod_all();
        break;
      case TOKEN_K_AND:
        enterOuterAlt(_localctx, 9);
        state = 937;
        traversalMethod_and();
        break;
      case TOKEN_K_ANY:
        enterOuterAlt(_localctx, 10);
        state = 938;
        traversalMethod_any();
        break;
      case TOKEN_K_AS:
        enterOuterAlt(_localctx, 11);
        state = 939;
        traversalMethod_as();
        break;
      case TOKEN_K_BARRIER:
        enterOuterAlt(_localctx, 12);
        state = 940;
        traversalMethod_barrier();
        break;
      case TOKEN_K_BOTH:
        enterOuterAlt(_localctx, 13);
        state = 941;
        traversalMethod_both();
        break;
      case TOKEN_K_BOTHE:
        enterOuterAlt(_localctx, 14);
        state = 942;
        traversalMethod_bothE();
        break;
      case TOKEN_K_BOTHV:
        enterOuterAlt(_localctx, 15);
        state = 943;
        traversalMethod_bothV();
        break;
      case TOKEN_K_BRANCH:
        enterOuterAlt(_localctx, 16);
        state = 944;
        traversalMethod_branch();
        break;
      case TOKEN_K_BY:
        enterOuterAlt(_localctx, 17);
        state = 945;
        traversalMethod_by();
        break;
      case TOKEN_K_CAP:
        enterOuterAlt(_localctx, 18);
        state = 946;
        traversalMethod_cap();
        break;
      case TOKEN_K_CHOOSE:
        enterOuterAlt(_localctx, 19);
        state = 947;
        traversalMethod_choose();
        break;
      case TOKEN_K_COALESCE:
        enterOuterAlt(_localctx, 20);
        state = 948;
        traversalMethod_coalesce();
        break;
      case TOKEN_K_COIN:
        enterOuterAlt(_localctx, 21);
        state = 949;
        traversalMethod_coin();
        break;
      case TOKEN_K_CONJOIN:
        enterOuterAlt(_localctx, 22);
        state = 950;
        traversalMethod_conjoin();
        break;
      case TOKEN_K_CONNECTEDCOMPONENT:
        enterOuterAlt(_localctx, 23);
        state = 951;
        traversalMethod_connectedComponent();
        break;
      case TOKEN_K_CONSTANT:
        enterOuterAlt(_localctx, 24);
        state = 952;
        traversalMethod_constant();
        break;
      case TOKEN_K_COUNT:
        enterOuterAlt(_localctx, 25);
        state = 953;
        traversalMethod_count();
        break;
      case TOKEN_K_CYCLICPATH:
        enterOuterAlt(_localctx, 26);
        state = 954;
        traversalMethod_cyclicPath();
        break;
      case TOKEN_K_DEDUP:
        enterOuterAlt(_localctx, 27);
        state = 955;
        traversalMethod_dedup();
        break;
      case TOKEN_K_DIFFERENCE:
        enterOuterAlt(_localctx, 28);
        state = 956;
        traversalMethod_difference();
        break;
      case TOKEN_K_DISCARD:
        enterOuterAlt(_localctx, 29);
        state = 957;
        traversalMethod_discard();
        break;
      case TOKEN_K_DISJUNCT:
        enterOuterAlt(_localctx, 30);
        state = 958;
        traversalMethod_disjunct();
        break;
      case TOKEN_K_DROP:
        enterOuterAlt(_localctx, 31);
        state = 959;
        traversalMethod_drop();
        break;
      case TOKEN_K_ELEMENTMAP:
        enterOuterAlt(_localctx, 32);
        state = 960;
        traversalMethod_elementMap();
        break;
      case TOKEN_K_EMIT:
        enterOuterAlt(_localctx, 33);
        state = 961;
        traversalMethod_emit();
        break;
      case TOKEN_K_FILTER:
        enterOuterAlt(_localctx, 34);
        state = 962;
        traversalMethod_filter();
        break;
      case TOKEN_K_FLATMAP:
        enterOuterAlt(_localctx, 35);
        state = 963;
        traversalMethod_flatMap();
        break;
      case TOKEN_K_FOLD:
        enterOuterAlt(_localctx, 36);
        state = 964;
        traversalMethod_fold();
        break;
      case TOKEN_K_FROM:
        enterOuterAlt(_localctx, 37);
        state = 965;
        traversalMethod_from();
        break;
      case TOKEN_K_GROUP:
        enterOuterAlt(_localctx, 38);
        state = 966;
        traversalMethod_group();
        break;
      case TOKEN_K_GROUPCOUNT:
        enterOuterAlt(_localctx, 39);
        state = 967;
        traversalMethod_groupCount();
        break;
      case TOKEN_K_HAS:
        enterOuterAlt(_localctx, 40);
        state = 968;
        traversalMethod_has();
        break;
      case TOKEN_K_HASID:
        enterOuterAlt(_localctx, 41);
        state = 969;
        traversalMethod_hasId();
        break;
      case TOKEN_K_HASKEY:
        enterOuterAlt(_localctx, 42);
        state = 970;
        traversalMethod_hasKey();
        break;
      case TOKEN_K_HASLABEL:
        enterOuterAlt(_localctx, 43);
        state = 971;
        traversalMethod_hasLabel();
        break;
      case TOKEN_K_HASNOT:
        enterOuterAlt(_localctx, 44);
        state = 972;
        traversalMethod_hasNot();
        break;
      case TOKEN_K_HASVALUE:
        enterOuterAlt(_localctx, 45);
        state = 973;
        traversalMethod_hasValue();
        break;
      case TOKEN_K_ID:
        enterOuterAlt(_localctx, 46);
        state = 974;
        traversalMethod_id();
        break;
      case TOKEN_K_IDENTITY:
        enterOuterAlt(_localctx, 47);
        state = 975;
        traversalMethod_identity();
        break;
      case TOKEN_K_IN:
        enterOuterAlt(_localctx, 48);
        state = 976;
        traversalMethod_in();
        break;
      case TOKEN_K_INE:
        enterOuterAlt(_localctx, 49);
        state = 977;
        traversalMethod_inE();
        break;
      case TOKEN_K_INTERSECT:
        enterOuterAlt(_localctx, 50);
        state = 978;
        traversalMethod_intersect();
        break;
      case TOKEN_K_INV:
        enterOuterAlt(_localctx, 51);
        state = 979;
        traversalMethod_inV();
        break;
      case TOKEN_K_INDEX:
        enterOuterAlt(_localctx, 52);
        state = 980;
        traversalMethod_index();
        break;
      case TOKEN_K_INJECT:
        enterOuterAlt(_localctx, 53);
        state = 981;
        traversalMethod_inject();
        break;
      case TOKEN_K_IS:
        enterOuterAlt(_localctx, 54);
        state = 982;
        traversalMethod_is();
        break;
      case TOKEN_K_KEY:
        enterOuterAlt(_localctx, 55);
        state = 983;
        traversalMethod_key();
        break;
      case TOKEN_K_LABEL:
        enterOuterAlt(_localctx, 56);
        state = 984;
        traversalMethod_label();
        break;
      case TOKEN_K_LIMIT:
        enterOuterAlt(_localctx, 57);
        state = 985;
        traversalMethod_limit();
        break;
      case TOKEN_K_LOCAL:
        enterOuterAlt(_localctx, 58);
        state = 986;
        traversalMethod_local();
        break;
      case TOKEN_K_LOOPS:
        enterOuterAlt(_localctx, 59);
        state = 987;
        traversalMethod_loops();
        break;
      case TOKEN_K_MAP:
        enterOuterAlt(_localctx, 60);
        state = 988;
        traversalMethod_map();
        break;
      case TOKEN_K_MATCH:
        enterOuterAlt(_localctx, 61);
        state = 989;
        traversalMethod_match();
        break;
      case TOKEN_K_MATH:
        enterOuterAlt(_localctx, 62);
        state = 990;
        traversalMethod_math();
        break;
      case TOKEN_K_MAX:
        enterOuterAlt(_localctx, 63);
        state = 991;
        traversalMethod_max();
        break;
      case TOKEN_K_MEAN:
        enterOuterAlt(_localctx, 64);
        state = 992;
        traversalMethod_mean();
        break;
      case TOKEN_K_MIN:
        enterOuterAlt(_localctx, 65);
        state = 993;
        traversalMethod_min();
        break;
      case TOKEN_K_NONE:
        enterOuterAlt(_localctx, 66);
        state = 994;
        traversalMethod_none();
        break;
      case TOKEN_K_NOT:
        enterOuterAlt(_localctx, 67);
        state = 995;
        traversalMethod_not();
        break;
      case TOKEN_K_OPTION:
        enterOuterAlt(_localctx, 68);
        state = 996;
        traversalMethod_option();
        break;
      case TOKEN_K_OPTIONAL:
        enterOuterAlt(_localctx, 69);
        state = 997;
        traversalMethod_optional();
        break;
      case TOKEN_K_OR:
        enterOuterAlt(_localctx, 70);
        state = 998;
        traversalMethod_or();
        break;
      case TOKEN_K_ORDER:
        enterOuterAlt(_localctx, 71);
        state = 999;
        traversalMethod_order();
        break;
      case TOKEN_K_OTHERV:
        enterOuterAlt(_localctx, 72);
        state = 1000;
        traversalMethod_otherV();
        break;
      case TOKEN_K_OUT:
        enterOuterAlt(_localctx, 73);
        state = 1001;
        traversalMethod_out();
        break;
      case TOKEN_K_OUTE:
        enterOuterAlt(_localctx, 74);
        state = 1002;
        traversalMethod_outE();
        break;
      case TOKEN_K_OUTV:
        enterOuterAlt(_localctx, 75);
        state = 1003;
        traversalMethod_outV();
        break;
      case TOKEN_K_PAGERANK:
        enterOuterAlt(_localctx, 76);
        state = 1004;
        traversalMethod_pageRank();
        break;
      case TOKEN_K_PATH:
        enterOuterAlt(_localctx, 77);
        state = 1005;
        traversalMethod_path();
        break;
      case TOKEN_K_PEERPRESSURE:
        enterOuterAlt(_localctx, 78);
        state = 1006;
        traversalMethod_peerPressure();
        break;
      case TOKEN_K_PROFILE:
        enterOuterAlt(_localctx, 79);
        state = 1007;
        traversalMethod_profile();
        break;
      case TOKEN_K_PROJECT:
        enterOuterAlt(_localctx, 80);
        state = 1008;
        traversalMethod_project();
        break;
      case TOKEN_K_PROPERTIES:
        enterOuterAlt(_localctx, 81);
        state = 1009;
        traversalMethod_properties();
        break;
      case TOKEN_K_PROPERTY:
        enterOuterAlt(_localctx, 82);
        state = 1010;
        traversalMethod_property();
        break;
      case TOKEN_K_PROPERTYMAP:
        enterOuterAlt(_localctx, 83);
        state = 1011;
        traversalMethod_propertyMap();
        break;
      case TOKEN_K_RANGE:
        enterOuterAlt(_localctx, 84);
        state = 1012;
        traversalMethod_range();
        break;
      case TOKEN_K_READ:
        enterOuterAlt(_localctx, 85);
        state = 1013;
        traversalMethod_read();
        break;
      case TOKEN_K_REPEAT:
        enterOuterAlt(_localctx, 86);
        state = 1014;
        traversalMethod_repeat();
        break;
      case TOKEN_K_SACK:
        enterOuterAlt(_localctx, 87);
        state = 1015;
        traversalMethod_sack();
        break;
      case TOKEN_K_SAMPLE:
        enterOuterAlt(_localctx, 88);
        state = 1016;
        traversalMethod_sample();
        break;
      case TOKEN_K_SELECT:
        enterOuterAlt(_localctx, 89);
        state = 1017;
        traversalMethod_select();
        break;
      case TOKEN_K_COMBINE:
        enterOuterAlt(_localctx, 90);
        state = 1018;
        traversalMethod_combine();
        break;
      case TOKEN_K_PRODUCT:
        enterOuterAlt(_localctx, 91);
        state = 1019;
        traversalMethod_product();
        break;
      case TOKEN_K_MERGE:
        enterOuterAlt(_localctx, 92);
        state = 1020;
        traversalMethod_merge();
        break;
      case TOKEN_K_SHORTESTPATH:
        enterOuterAlt(_localctx, 93);
        state = 1021;
        traversalMethod_shortestPath();
        break;
      case TOKEN_K_SIDEEFFECT:
        enterOuterAlt(_localctx, 94);
        state = 1022;
        traversalMethod_sideEffect();
        break;
      case TOKEN_K_SIMPLEPATH:
        enterOuterAlt(_localctx, 95);
        state = 1023;
        traversalMethod_simplePath();
        break;
      case TOKEN_K_SKIP:
        enterOuterAlt(_localctx, 96);
        state = 1024;
        traversalMethod_skip();
        break;
      case TOKEN_K_SUBGRAPH:
        enterOuterAlt(_localctx, 97);
        state = 1025;
        traversalMethod_subgraph();
        break;
      case TOKEN_K_SUM:
        enterOuterAlt(_localctx, 98);
        state = 1026;
        traversalMethod_sum();
        break;
      case TOKEN_K_TAIL:
        enterOuterAlt(_localctx, 99);
        state = 1027;
        traversalMethod_tail();
        break;
      case TOKEN_K_FAIL:
        enterOuterAlt(_localctx, 100);
        state = 1028;
        traversalMethod_fail();
        break;
      case TOKEN_K_TIMELIMIT:
        enterOuterAlt(_localctx, 101);
        state = 1029;
        traversalMethod_timeLimit();
        break;
      case TOKEN_K_TIMES:
        enterOuterAlt(_localctx, 102);
        state = 1030;
        traversalMethod_times();
        break;
      case TOKEN_K_TO:
        enterOuterAlt(_localctx, 103);
        state = 1031;
        traversalMethod_to();
        break;
      case TOKEN_K_TOE:
        enterOuterAlt(_localctx, 104);
        state = 1032;
        traversalMethod_toE();
        break;
      case TOKEN_K_TOV:
        enterOuterAlt(_localctx, 105);
        state = 1033;
        traversalMethod_toV();
        break;
      case TOKEN_K_TREE:
        enterOuterAlt(_localctx, 106);
        state = 1034;
        traversalMethod_tree();
        break;
      case TOKEN_K_UNFOLD:
        enterOuterAlt(_localctx, 107);
        state = 1035;
        traversalMethod_unfold();
        break;
      case TOKEN_K_UNION:
        enterOuterAlt(_localctx, 108);
        state = 1036;
        traversalMethod_union();
        break;
      case TOKEN_K_UNTIL:
        enterOuterAlt(_localctx, 109);
        state = 1037;
        traversalMethod_until();
        break;
      case TOKEN_K_VALUE:
        enterOuterAlt(_localctx, 110);
        state = 1038;
        traversalMethod_value();
        break;
      case TOKEN_K_VALUEMAP:
        enterOuterAlt(_localctx, 111);
        state = 1039;
        traversalMethod_valueMap();
        break;
      case TOKEN_K_VALUES:
        enterOuterAlt(_localctx, 112);
        state = 1040;
        traversalMethod_values();
        break;
      case TOKEN_K_WHERE:
        enterOuterAlt(_localctx, 113);
        state = 1041;
        traversalMethod_where();
        break;
      case TOKEN_K_WITH:
        enterOuterAlt(_localctx, 114);
        state = 1042;
        traversalMethod_with();
        break;
      case TOKEN_K_WRITE:
        enterOuterAlt(_localctx, 115);
        state = 1043;
        traversalMethod_write();
        break;
      case TOKEN_K_ELEMENT:
        enterOuterAlt(_localctx, 116);
        state = 1044;
        traversalMethod_element();
        break;
      case TOKEN_K_CALL:
        enterOuterAlt(_localctx, 117);
        state = 1045;
        traversalMethod_call();
        break;
      case TOKEN_K_CONCAT:
        enterOuterAlt(_localctx, 118);
        state = 1046;
        traversalMethod_concat();
        break;
      case TOKEN_K_ASSTRING:
        enterOuterAlt(_localctx, 119);
        state = 1047;
        traversalMethod_asString();
        break;
      case TOKEN_K_FORMAT:
        enterOuterAlt(_localctx, 120);
        state = 1048;
        traversalMethod_format();
        break;
      case TOKEN_K_TOUPPER:
        enterOuterAlt(_localctx, 121);
        state = 1049;
        traversalMethod_toUpper();
        break;
      case TOKEN_K_TOLOWER:
        enterOuterAlt(_localctx, 122);
        state = 1050;
        traversalMethod_toLower();
        break;
      case TOKEN_K_LENGTH:
        enterOuterAlt(_localctx, 123);
        state = 1051;
        traversalMethod_length();
        break;
      case TOKEN_K_TRIM:
        enterOuterAlt(_localctx, 124);
        state = 1052;
        traversalMethod_trim();
        break;
      case TOKEN_K_LTRIM:
        enterOuterAlt(_localctx, 125);
        state = 1053;
        traversalMethod_lTrim();
        break;
      case TOKEN_K_RTRIM:
        enterOuterAlt(_localctx, 126);
        state = 1054;
        traversalMethod_rTrim();
        break;
      case TOKEN_K_REVERSE:
        enterOuterAlt(_localctx, 127);
        state = 1055;
        traversalMethod_reverse();
        break;
      case TOKEN_K_REPLACE:
        enterOuterAlt(_localctx, 128);
        state = 1056;
        traversalMethod_replace();
        break;
      case TOKEN_K_SPLIT:
        enterOuterAlt(_localctx, 129);
        state = 1057;
        traversalMethod_split();
        break;
      case TOKEN_K_SUBSTRING:
        enterOuterAlt(_localctx, 130);
        state = 1058;
        traversalMethod_substring();
        break;
      case TOKEN_K_ASBOOL:
        enterOuterAlt(_localctx, 131);
        state = 1059;
        traversalMethod_asBool();
        break;
      case TOKEN_K_ASDATE:
        enterOuterAlt(_localctx, 132);
        state = 1060;
        traversalMethod_asDate();
        break;
      case TOKEN_K_DATEADD:
        enterOuterAlt(_localctx, 133);
        state = 1061;
        traversalMethod_dateAdd();
        break;
      case TOKEN_K_DATEDIFF:
        enterOuterAlt(_localctx, 134);
        state = 1062;
        traversalMethod_dateDiff();
        break;
      case TOKEN_K_ASNUMBER:
        enterOuterAlt(_localctx, 135);
        state = 1063;
        traversalMethod_asNumber();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_VContext traversalMethod_V() {
    dynamic _localctx = TraversalMethod_VContext(context, state);
    enterRule(_localctx, 58, RULE_traversalMethod_V);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1066;
      match(TOKEN_K_V);
      state = 1067;
      match(TOKEN_LPAREN);
      state = 1068;
      genericArgumentVarargs();
      state = 1069;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_EContext traversalMethod_E() {
    dynamic _localctx = TraversalMethod_EContext(context, state);
    enterRule(_localctx, 60, RULE_traversalMethod_E);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1071;
      match(TOKEN_K_E);
      state = 1072;
      match(TOKEN_LPAREN);
      state = 1073;
      genericArgumentVarargs();
      state = 1074;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_addEContext traversalMethod_addE() {
    dynamic _localctx = TraversalMethod_addEContext(context, state);
    enterRule(_localctx, 62, RULE_traversalMethod_addE);
    try {
      state = 1086;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 24, context)) {
      case 1:
        _localctx = TraversalMethod_addE_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1076;
        match(TOKEN_K_ADDE);
        state = 1077;
        match(TOKEN_LPAREN);
        state = 1078;
        stringArgument();
        state = 1079;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_addE_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1081;
        match(TOKEN_K_ADDE);
        state = 1082;
        match(TOKEN_LPAREN);
        state = 1083;
        nestedTraversal();
        state = 1084;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_addVContext traversalMethod_addV() {
    dynamic _localctx = TraversalMethod_addVContext(context, state);
    enterRule(_localctx, 64, RULE_traversalMethod_addV);
    try {
      state = 1101;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 25, context)) {
      case 1:
        _localctx = TraversalMethod_addV_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1088;
        match(TOKEN_K_ADDV);
        state = 1089;
        match(TOKEN_LPAREN);
        state = 1090;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_addV_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1091;
        match(TOKEN_K_ADDV);
        state = 1092;
        match(TOKEN_LPAREN);
        state = 1093;
        stringArgument();
        state = 1094;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_addV_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1096;
        match(TOKEN_K_ADDV);
        state = 1097;
        match(TOKEN_LPAREN);
        state = 1098;
        nestedTraversal();
        state = 1099;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_aggregateContext traversalMethod_aggregate() {
    dynamic _localctx = TraversalMethod_aggregateContext(context, state);
    enterRule(_localctx, 66, RULE_traversalMethod_aggregate);
    try {
      _localctx = TraversalMethod_aggregate_StringContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1103;
      match(TOKEN_K_AGGREGATE);
      state = 1104;
      match(TOKEN_LPAREN);
      state = 1105;
      stringLiteral();
      state = 1106;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_allContext traversalMethod_all() {
    dynamic _localctx = TraversalMethod_allContext(context, state);
    enterRule(_localctx, 68, RULE_traversalMethod_all);
    try {
      _localctx = TraversalMethod_all_PContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1108;
      match(TOKEN_K_ALL);
      state = 1109;
      match(TOKEN_LPAREN);
      state = 1110;
      traversalPredicate(0);
      state = 1111;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_andContext traversalMethod_and() {
    dynamic _localctx = TraversalMethod_andContext(context, state);
    enterRule(_localctx, 70, RULE_traversalMethod_and);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1113;
      match(TOKEN_K_AND);
      state = 1114;
      match(TOKEN_LPAREN);
      state = 1115;
      nestedTraversalList();
      state = 1116;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_anyContext traversalMethod_any() {
    dynamic _localctx = TraversalMethod_anyContext(context, state);
    enterRule(_localctx, 72, RULE_traversalMethod_any);
    try {
      _localctx = TraversalMethod_any_PContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1118;
      match(TOKEN_K_ANY);
      state = 1119;
      match(TOKEN_LPAREN);
      state = 1120;
      traversalPredicate(0);
      state = 1121;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_asContext traversalMethod_as() {
    dynamic _localctx = TraversalMethod_asContext(context, state);
    enterRule(_localctx, 74, RULE_traversalMethod_as);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 1123;
      match(TOKEN_K_AS);
      state = 1124;
      match(TOKEN_LPAREN);
      state = 1125;
      stringLiteral();
      state = 1128;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 1126;
        match(TOKEN_COMMA);
        state = 1127;
        stringNullableLiteralVarargs();
      }

      state = 1130;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_asBoolContext traversalMethod_asBool() {
    dynamic _localctx = TraversalMethod_asBoolContext(context, state);
    enterRule(_localctx, 76, RULE_traversalMethod_asBool);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1132;
      match(TOKEN_K_ASBOOL);
      state = 1133;
      match(TOKEN_LPAREN);
      state = 1134;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_asDateContext traversalMethod_asDate() {
    dynamic _localctx = TraversalMethod_asDateContext(context, state);
    enterRule(_localctx, 78, RULE_traversalMethod_asDate);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1136;
      match(TOKEN_K_ASDATE);
      state = 1137;
      match(TOKEN_LPAREN);
      state = 1138;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_asNumberContext traversalMethod_asNumber() {
    dynamic _localctx = TraversalMethod_asNumberContext(context, state);
    enterRule(_localctx, 80, RULE_traversalMethod_asNumber);
    try {
      state = 1148;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 27, context)) {
      case 1:
        _localctx = TraversalMethod_asNumber_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1140;
        match(TOKEN_K_ASNUMBER);
        state = 1141;
        match(TOKEN_LPAREN);
        state = 1142;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_asNumber_traversalGTypeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1143;
        match(TOKEN_K_ASNUMBER);
        state = 1144;
        match(TOKEN_LPAREN);
        state = 1145;
        traversalGType();
        state = 1146;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_asStringContext traversalMethod_asString() {
    dynamic _localctx = TraversalMethod_asStringContext(context, state);
    enterRule(_localctx, 82, RULE_traversalMethod_asString);
    try {
      state = 1158;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 28, context)) {
      case 1:
        _localctx = TraversalMethod_asString_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1150;
        match(TOKEN_K_ASSTRING);
        state = 1151;
        match(TOKEN_LPAREN);
        state = 1152;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_asString_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1153;
        match(TOKEN_K_ASSTRING);
        state = 1154;
        match(TOKEN_LPAREN);
        state = 1155;
        traversalScope();
        state = 1156;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_barrierContext traversalMethod_barrier() {
    dynamic _localctx = TraversalMethod_barrierContext(context, state);
    enterRule(_localctx, 84, RULE_traversalMethod_barrier);
    try {
      state = 1173;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 29, context)) {
      case 1:
        _localctx = TraversalMethod_barrier_ConsumerContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1160;
        match(TOKEN_K_BARRIER);
        state = 1161;
        match(TOKEN_LPAREN);
        state = 1162;
        traversalSackMethod();
        state = 1163;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_barrier_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1165;
        match(TOKEN_K_BARRIER);
        state = 1166;
        match(TOKEN_LPAREN);
        state = 1167;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_barrier_intContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1168;
        match(TOKEN_K_BARRIER);
        state = 1169;
        match(TOKEN_LPAREN);
        state = 1170;
        integerLiteral();
        state = 1171;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_bothContext traversalMethod_both() {
    dynamic _localctx = TraversalMethod_bothContext(context, state);
    enterRule(_localctx, 86, RULE_traversalMethod_both);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1175;
      match(TOKEN_K_BOTH);
      state = 1176;
      match(TOKEN_LPAREN);
      state = 1177;
      stringNullableArgumentVarargs();
      state = 1178;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_bothEContext traversalMethod_bothE() {
    dynamic _localctx = TraversalMethod_bothEContext(context, state);
    enterRule(_localctx, 88, RULE_traversalMethod_bothE);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1180;
      match(TOKEN_K_BOTHE);
      state = 1181;
      match(TOKEN_LPAREN);
      state = 1182;
      stringNullableArgumentVarargs();
      state = 1183;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_bothVContext traversalMethod_bothV() {
    dynamic _localctx = TraversalMethod_bothVContext(context, state);
    enterRule(_localctx, 90, RULE_traversalMethod_bothV);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1185;
      match(TOKEN_K_BOTHV);
      state = 1186;
      match(TOKEN_LPAREN);
      state = 1187;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_branchContext traversalMethod_branch() {
    dynamic _localctx = TraversalMethod_branchContext(context, state);
    enterRule(_localctx, 92, RULE_traversalMethod_branch);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1189;
      match(TOKEN_K_BRANCH);
      state = 1190;
      match(TOKEN_LPAREN);
      state = 1191;
      nestedTraversal();
      state = 1192;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_byContext traversalMethod_by() {
    dynamic _localctx = TraversalMethod_byContext(context, state);
    enterRule(_localctx, 94, RULE_traversalMethod_by);
    try {
      state = 1248;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 30, context)) {
      case 1:
        _localctx = TraversalMethod_by_ComparatorContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1194;
        match(TOKEN_K_BY);
        state = 1195;
        match(TOKEN_LPAREN);
        state = 1196;
        traversalComparator();
        state = 1197;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_by_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1199;
        match(TOKEN_K_BY);
        state = 1200;
        match(TOKEN_LPAREN);
        state = 1201;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_by_FunctionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1202;
        match(TOKEN_K_BY);
        state = 1203;
        match(TOKEN_LPAREN);
        state = 1204;
        traversalFunction();
        state = 1205;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_by_Function_ComparatorContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 1207;
        match(TOKEN_K_BY);
        state = 1208;
        match(TOKEN_LPAREN);
        state = 1209;
        traversalFunction();
        state = 1210;
        match(TOKEN_COMMA);
        state = 1211;
        traversalComparator();
        state = 1212;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalMethod_by_OrderContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 1214;
        match(TOKEN_K_BY);
        state = 1215;
        match(TOKEN_LPAREN);
        state = 1216;
        traversalOrder();
        state = 1217;
        match(TOKEN_RPAREN);
        break;
      case 6:
        _localctx = TraversalMethod_by_StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        state = 1219;
        match(TOKEN_K_BY);
        state = 1220;
        match(TOKEN_LPAREN);
        state = 1221;
        stringLiteral();
        state = 1222;
        match(TOKEN_RPAREN);
        break;
      case 7:
        _localctx = TraversalMethod_by_String_ComparatorContext(_localctx);
        enterOuterAlt(_localctx, 7);
        state = 1224;
        match(TOKEN_K_BY);
        state = 1225;
        match(TOKEN_LPAREN);
        state = 1226;
        stringLiteral();
        state = 1227;
        match(TOKEN_COMMA);
        state = 1228;
        traversalComparator();
        state = 1229;
        match(TOKEN_RPAREN);
        break;
      case 8:
        _localctx = TraversalMethod_by_TContext(_localctx);
        enterOuterAlt(_localctx, 8);
        state = 1231;
        match(TOKEN_K_BY);
        state = 1232;
        match(TOKEN_LPAREN);
        state = 1233;
        traversalT();
        state = 1234;
        match(TOKEN_RPAREN);
        break;
      case 9:
        _localctx = TraversalMethod_by_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 9);
        state = 1236;
        match(TOKEN_K_BY);
        state = 1237;
        match(TOKEN_LPAREN);
        state = 1238;
        nestedTraversal();
        state = 1239;
        match(TOKEN_RPAREN);
        break;
      case 10:
        _localctx = TraversalMethod_by_Traversal_ComparatorContext(_localctx);
        enterOuterAlt(_localctx, 10);
        state = 1241;
        match(TOKEN_K_BY);
        state = 1242;
        match(TOKEN_LPAREN);
        state = 1243;
        nestedTraversal();
        state = 1244;
        match(TOKEN_COMMA);
        state = 1245;
        traversalComparator();
        state = 1246;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_callContext traversalMethod_call() {
    dynamic _localctx = TraversalMethod_callContext(context, state);
    enterRule(_localctx, 96, RULE_traversalMethod_call);
    try {
      state = 1278;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 31, context)) {
      case 1:
        _localctx = TraversalMethod_call_stringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1250;
        match(TOKEN_K_CALL);
        state = 1251;
        match(TOKEN_LPAREN);
        state = 1252;
        stringLiteral();
        state = 1253;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_call_string_mapContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1255;
        match(TOKEN_K_CALL);
        state = 1256;
        match(TOKEN_LPAREN);
        state = 1257;
        stringLiteral();
        state = 1258;
        match(TOKEN_COMMA);
        state = 1259;
        genericMapArgument();
        state = 1260;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_call_string_traversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1262;
        match(TOKEN_K_CALL);
        state = 1263;
        match(TOKEN_LPAREN);
        state = 1264;
        stringLiteral();
        state = 1265;
        match(TOKEN_COMMA);
        state = 1266;
        nestedTraversal();
        state = 1267;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_call_string_map_traversalContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 1269;
        match(TOKEN_K_CALL);
        state = 1270;
        match(TOKEN_LPAREN);
        state = 1271;
        stringLiteral();
        state = 1272;
        match(TOKEN_COMMA);
        state = 1273;
        genericMapArgument();
        state = 1274;
        match(TOKEN_COMMA);
        state = 1275;
        nestedTraversal();
        state = 1276;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_capContext traversalMethod_cap() {
    dynamic _localctx = TraversalMethod_capContext(context, state);
    enterRule(_localctx, 98, RULE_traversalMethod_cap);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 1280;
      match(TOKEN_K_CAP);
      state = 1281;
      match(TOKEN_LPAREN);
      state = 1282;
      stringLiteral();
      state = 1285;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 1283;
        match(TOKEN_COMMA);
        state = 1284;
        stringNullableLiteralVarargs();
      }

      state = 1287;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_chooseContext traversalMethod_choose() {
    dynamic _localctx = TraversalMethod_chooseContext(context, state);
    enterRule(_localctx, 100, RULE_traversalMethod_choose);
    try {
      state = 1331;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 33, context)) {
      case 1:
        _localctx = TraversalMethod_choose_FunctionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1289;
        match(TOKEN_K_CHOOSE);
        state = 1290;
        match(TOKEN_LPAREN);
        state = 1291;
        traversalFunction();
        state = 1292;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_choose_Predicate_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1294;
        match(TOKEN_K_CHOOSE);
        state = 1295;
        match(TOKEN_LPAREN);
        state = 1296;
        traversalPredicate(0);
        state = 1297;
        match(TOKEN_COMMA);
        state = 1298;
        nestedTraversal();
        state = 1299;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_choose_Predicate_Traversal_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1301;
        match(TOKEN_K_CHOOSE);
        state = 1302;
        match(TOKEN_LPAREN);
        state = 1303;
        traversalPredicate(0);
        state = 1304;
        match(TOKEN_COMMA);
        state = 1305;
        nestedTraversal();
        state = 1306;
        match(TOKEN_COMMA);
        state = 1307;
        nestedTraversal();
        state = 1308;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_choose_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 1310;
        match(TOKEN_K_CHOOSE);
        state = 1311;
        match(TOKEN_LPAREN);
        state = 1312;
        nestedTraversal();
        state = 1313;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalMethod_choose_Traversal_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 1315;
        match(TOKEN_K_CHOOSE);
        state = 1316;
        match(TOKEN_LPAREN);
        state = 1317;
        nestedTraversal();
        state = 1318;
        match(TOKEN_COMMA);
        state = 1319;
        nestedTraversal();
        state = 1320;
        match(TOKEN_RPAREN);
        break;
      case 6:
        _localctx = TraversalMethod_choose_Traversal_Traversal_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 6);
        state = 1322;
        match(TOKEN_K_CHOOSE);
        state = 1323;
        match(TOKEN_LPAREN);
        state = 1324;
        nestedTraversal();
        state = 1325;
        match(TOKEN_COMMA);
        state = 1326;
        nestedTraversal();
        state = 1327;
        match(TOKEN_COMMA);
        state = 1328;
        nestedTraversal();
        state = 1329;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_coalesceContext traversalMethod_coalesce() {
    dynamic _localctx = TraversalMethod_coalesceContext(context, state);
    enterRule(_localctx, 102, RULE_traversalMethod_coalesce);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1333;
      match(TOKEN_K_COALESCE);
      state = 1334;
      match(TOKEN_LPAREN);
      state = 1335;
      nestedTraversalList();
      state = 1336;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_coinContext traversalMethod_coin() {
    dynamic _localctx = TraversalMethod_coinContext(context, state);
    enterRule(_localctx, 104, RULE_traversalMethod_coin);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1338;
      match(TOKEN_K_COIN);
      state = 1339;
      match(TOKEN_LPAREN);
      state = 1340;
      numericLiteral();
      state = 1341;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_combineContext traversalMethod_combine() {
    dynamic _localctx = TraversalMethod_combineContext(context, state);
    enterRule(_localctx, 106, RULE_traversalMethod_combine);
    try {
      _localctx = TraversalMethod_combine_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1343;
      match(TOKEN_K_COMBINE);
      state = 1344;
      match(TOKEN_LPAREN);
      state = 1345;
      genericLiteral();
      state = 1346;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_concatContext traversalMethod_concat() {
    dynamic _localctx = TraversalMethod_concatContext(context, state);
    enterRule(_localctx, 108, RULE_traversalMethod_concat);
    int _la;
    try {
      state = 1362;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 35, context)) {
      case 1:
        _localctx = TraversalMethod_concat_Traversal_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1348;
        match(TOKEN_K_CONCAT);
        state = 1349;
        match(TOKEN_LPAREN);
        state = 1350;
        nestedTraversal();
        state = 1353;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1351;
          match(TOKEN_COMMA);
          state = 1352;
          nestedTraversalList();
        }

        state = 1355;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_concat_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1357;
        match(TOKEN_K_CONCAT);
        state = 1358;
        match(TOKEN_LPAREN);
        state = 1359;
        stringNullableLiteralVarargs();
        state = 1360;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_conjoinContext traversalMethod_conjoin() {
    dynamic _localctx = TraversalMethod_conjoinContext(context, state);
    enterRule(_localctx, 110, RULE_traversalMethod_conjoin);
    try {
      _localctx = TraversalMethod_conjoin_StringContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1364;
      match(TOKEN_K_CONJOIN);
      state = 1365;
      match(TOKEN_LPAREN);
      state = 1366;
      stringLiteral();
      state = 1367;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_connectedComponentContext traversalMethod_connectedComponent() {
    dynamic _localctx = TraversalMethod_connectedComponentContext(context, state);
    enterRule(_localctx, 112, RULE_traversalMethod_connectedComponent);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1369;
      match(TOKEN_K_CONNECTEDCOMPONENT);
      state = 1370;
      match(TOKEN_LPAREN);
      state = 1371;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_constantContext traversalMethod_constant() {
    dynamic _localctx = TraversalMethod_constantContext(context, state);
    enterRule(_localctx, 114, RULE_traversalMethod_constant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1373;
      match(TOKEN_K_CONSTANT);
      state = 1374;
      match(TOKEN_LPAREN);
      state = 1375;
      genericLiteral();
      state = 1376;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_countContext traversalMethod_count() {
    dynamic _localctx = TraversalMethod_countContext(context, state);
    enterRule(_localctx, 116, RULE_traversalMethod_count);
    try {
      state = 1386;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 36, context)) {
      case 1:
        _localctx = TraversalMethod_count_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1378;
        match(TOKEN_K_COUNT);
        state = 1379;
        match(TOKEN_LPAREN);
        state = 1380;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_count_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1381;
        match(TOKEN_K_COUNT);
        state = 1382;
        match(TOKEN_LPAREN);
        state = 1383;
        traversalScope();
        state = 1384;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_cyclicPathContext traversalMethod_cyclicPath() {
    dynamic _localctx = TraversalMethod_cyclicPathContext(context, state);
    enterRule(_localctx, 118, RULE_traversalMethod_cyclicPath);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1388;
      match(TOKEN_K_CYCLICPATH);
      state = 1389;
      match(TOKEN_LPAREN);
      state = 1390;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_dateAddContext traversalMethod_dateAdd() {
    dynamic _localctx = TraversalMethod_dateAddContext(context, state);
    enterRule(_localctx, 120, RULE_traversalMethod_dateAdd);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1392;
      match(TOKEN_K_DATEADD);
      state = 1393;
      match(TOKEN_LPAREN);
      state = 1394;
      traversalDT();
      state = 1395;
      match(TOKEN_COMMA);
      state = 1396;
      integerLiteral();
      state = 1397;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_dateDiffContext traversalMethod_dateDiff() {
    dynamic _localctx = TraversalMethod_dateDiffContext(context, state);
    enterRule(_localctx, 122, RULE_traversalMethod_dateDiff);
    try {
      state = 1409;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 37, context)) {
      case 1:
        _localctx = TraversalMethod_dateDiff_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1399;
        match(TOKEN_K_DATEDIFF);
        state = 1400;
        match(TOKEN_LPAREN);
        state = 1401;
        nestedTraversal();
        state = 1402;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_dateDiff_DateContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1404;
        match(TOKEN_K_DATEDIFF);
        state = 1405;
        match(TOKEN_LPAREN);
        state = 1406;
        dateLiteral();
        state = 1407;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_dedupContext traversalMethod_dedup() {
    dynamic _localctx = TraversalMethod_dedupContext(context, state);
    enterRule(_localctx, 124, RULE_traversalMethod_dedup);
    int _la;
    try {
      state = 1425;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 39, context)) {
      case 1:
        _localctx = TraversalMethod_dedup_Scope_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1411;
        match(TOKEN_K_DEDUP);
        state = 1412;
        match(TOKEN_LPAREN);
        state = 1413;
        traversalScope();
        state = 1416;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1414;
          match(TOKEN_COMMA);
          state = 1415;
          stringNullableLiteralVarargs();
        }

        state = 1418;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_dedup_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1420;
        match(TOKEN_K_DEDUP);
        state = 1421;
        match(TOKEN_LPAREN);
        state = 1422;
        stringNullableLiteralVarargs();
        state = 1423;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_differenceContext traversalMethod_difference() {
    dynamic _localctx = TraversalMethod_differenceContext(context, state);
    enterRule(_localctx, 126, RULE_traversalMethod_difference);
    try {
      _localctx = TraversalMethod_difference_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1427;
      match(TOKEN_K_DIFFERENCE);
      state = 1428;
      match(TOKEN_LPAREN);
      state = 1429;
      genericLiteral();
      state = 1430;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_discardContext traversalMethod_discard() {
    dynamic _localctx = TraversalMethod_discardContext(context, state);
    enterRule(_localctx, 128, RULE_traversalMethod_discard);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1432;
      match(TOKEN_K_DISCARD);
      state = 1433;
      match(TOKEN_LPAREN);
      state = 1434;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_disjunctContext traversalMethod_disjunct() {
    dynamic _localctx = TraversalMethod_disjunctContext(context, state);
    enterRule(_localctx, 130, RULE_traversalMethod_disjunct);
    try {
      _localctx = TraversalMethod_disjunct_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1436;
      match(TOKEN_K_DISJUNCT);
      state = 1437;
      match(TOKEN_LPAREN);
      state = 1438;
      genericLiteral();
      state = 1439;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_dropContext traversalMethod_drop() {
    dynamic _localctx = TraversalMethod_dropContext(context, state);
    enterRule(_localctx, 132, RULE_traversalMethod_drop);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1441;
      match(TOKEN_K_DROP);
      state = 1442;
      match(TOKEN_LPAREN);
      state = 1443;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_elementContext traversalMethod_element() {
    dynamic _localctx = TraversalMethod_elementContext(context, state);
    enterRule(_localctx, 134, RULE_traversalMethod_element);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1445;
      match(TOKEN_K_ELEMENT);
      state = 1446;
      match(TOKEN_LPAREN);
      state = 1447;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_elementMapContext traversalMethod_elementMap() {
    dynamic _localctx = TraversalMethod_elementMapContext(context, state);
    enterRule(_localctx, 136, RULE_traversalMethod_elementMap);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1449;
      match(TOKEN_K_ELEMENTMAP);
      state = 1450;
      match(TOKEN_LPAREN);
      state = 1451;
      stringNullableLiteralVarargs();
      state = 1452;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_emitContext traversalMethod_emit() {
    dynamic _localctx = TraversalMethod_emitContext(context, state);
    enterRule(_localctx, 138, RULE_traversalMethod_emit);
    try {
      state = 1467;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 40, context)) {
      case 1:
        _localctx = TraversalMethod_emit_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1454;
        match(TOKEN_K_EMIT);
        state = 1455;
        match(TOKEN_LPAREN);
        state = 1456;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_emit_PredicateContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1457;
        match(TOKEN_K_EMIT);
        state = 1458;
        match(TOKEN_LPAREN);
        state = 1459;
        traversalPredicate(0);
        state = 1460;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_emit_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1462;
        match(TOKEN_K_EMIT);
        state = 1463;
        match(TOKEN_LPAREN);
        state = 1464;
        nestedTraversal();
        state = 1465;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_failContext traversalMethod_fail() {
    dynamic _localctx = TraversalMethod_failContext(context, state);
    enterRule(_localctx, 140, RULE_traversalMethod_fail);
    try {
      state = 1477;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 41, context)) {
      case 1:
        _localctx = TraversalMethod_fail_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1469;
        match(TOKEN_K_FAIL);
        state = 1470;
        match(TOKEN_LPAREN);
        state = 1471;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_fail_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1472;
        match(TOKEN_K_FAIL);
        state = 1473;
        match(TOKEN_LPAREN);
        state = 1474;
        stringLiteral();
        state = 1475;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_filterContext traversalMethod_filter() {
    dynamic _localctx = TraversalMethod_filterContext(context, state);
    enterRule(_localctx, 142, RULE_traversalMethod_filter);
    try {
      state = 1489;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 42, context)) {
      case 1:
        _localctx = TraversalMethod_filter_PredicateContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1479;
        match(TOKEN_K_FILTER);
        state = 1480;
        match(TOKEN_LPAREN);
        state = 1481;
        traversalPredicate(0);
        state = 1482;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_filter_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1484;
        match(TOKEN_K_FILTER);
        state = 1485;
        match(TOKEN_LPAREN);
        state = 1486;
        nestedTraversal();
        state = 1487;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_flatMapContext traversalMethod_flatMap() {
    dynamic _localctx = TraversalMethod_flatMapContext(context, state);
    enterRule(_localctx, 144, RULE_traversalMethod_flatMap);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1491;
      match(TOKEN_K_FLATMAP);
      state = 1492;
      match(TOKEN_LPAREN);
      state = 1493;
      nestedTraversal();
      state = 1494;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_foldContext traversalMethod_fold() {
    dynamic _localctx = TraversalMethod_foldContext(context, state);
    enterRule(_localctx, 146, RULE_traversalMethod_fold);
    try {
      state = 1506;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 43, context)) {
      case 1:
        _localctx = TraversalMethod_fold_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1496;
        match(TOKEN_K_FOLD);
        state = 1497;
        match(TOKEN_LPAREN);
        state = 1498;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_fold_Object_BiFunctionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1499;
        match(TOKEN_K_FOLD);
        state = 1500;
        match(TOKEN_LPAREN);
        state = 1501;
        genericLiteral();
        state = 1502;
        match(TOKEN_COMMA);
        state = 1503;
        traversalBiFunction();
        state = 1504;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_formatContext traversalMethod_format() {
    dynamic _localctx = TraversalMethod_formatContext(context, state);
    enterRule(_localctx, 148, RULE_traversalMethod_format);
    try {
      _localctx = TraversalMethod_format_StringContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1508;
      match(TOKEN_K_FORMAT);
      state = 1509;
      match(TOKEN_LPAREN);
      state = 1510;
      stringLiteral();
      state = 1511;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_fromContext traversalMethod_from() {
    dynamic _localctx = TraversalMethod_fromContext(context, state);
    enterRule(_localctx, 150, RULE_traversalMethod_from);
    try {
      state = 1523;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 44, context)) {
      case 1:
        _localctx = TraversalMethod_from_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1513;
        match(TOKEN_K_FROM);
        state = 1514;
        match(TOKEN_LPAREN);
        state = 1515;
        stringLiteral();
        state = 1516;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_from_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1518;
        match(TOKEN_K_FROM);
        state = 1519;
        match(TOKEN_LPAREN);
        state = 1520;
        nestedTraversal();
        state = 1521;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_groupContext traversalMethod_group() {
    dynamic _localctx = TraversalMethod_groupContext(context, state);
    enterRule(_localctx, 152, RULE_traversalMethod_group);
    try {
      state = 1533;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 45, context)) {
      case 1:
        _localctx = TraversalMethod_group_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1525;
        match(TOKEN_K_GROUP);
        state = 1526;
        match(TOKEN_LPAREN);
        state = 1527;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_group_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1528;
        match(TOKEN_K_GROUP);
        state = 1529;
        match(TOKEN_LPAREN);
        state = 1530;
        stringLiteral();
        state = 1531;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_groupCountContext traversalMethod_groupCount() {
    dynamic _localctx = TraversalMethod_groupCountContext(context, state);
    enterRule(_localctx, 154, RULE_traversalMethod_groupCount);
    try {
      state = 1543;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 46, context)) {
      case 1:
        _localctx = TraversalMethod_groupCount_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1535;
        match(TOKEN_K_GROUPCOUNT);
        state = 1536;
        match(TOKEN_LPAREN);
        state = 1537;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_groupCount_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1538;
        match(TOKEN_K_GROUPCOUNT);
        state = 1539;
        match(TOKEN_LPAREN);
        state = 1540;
        stringLiteral();
        state = 1541;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasContext traversalMethod_has() {
    dynamic _localctx = TraversalMethod_hasContext(context, state);
    enterRule(_localctx, 156, RULE_traversalMethod_has);
    try {
      state = 1596;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 47, context)) {
      case 1:
        _localctx = TraversalMethod_has_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1545;
        match(TOKEN_K_HAS);
        state = 1546;
        match(TOKEN_LPAREN);
        state = 1547;
        stringNullableLiteral();
        state = 1548;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_has_String_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1550;
        match(TOKEN_K_HAS);
        state = 1551;
        match(TOKEN_LPAREN);
        state = 1552;
        stringNullableLiteral();
        state = 1553;
        match(TOKEN_COMMA);
        state = 1554;
        genericArgument();
        state = 1555;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_has_String_PContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1557;
        match(TOKEN_K_HAS);
        state = 1558;
        match(TOKEN_LPAREN);
        state = 1559;
        stringNullableLiteral();
        state = 1560;
        match(TOKEN_COMMA);
        state = 1561;
        traversalPredicate(0);
        state = 1562;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_has_String_String_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 1564;
        match(TOKEN_K_HAS);
        state = 1565;
        match(TOKEN_LPAREN);
        state = 1566;
        stringNullableArgument();
        state = 1567;
        match(TOKEN_COMMA);
        state = 1568;
        stringNullableLiteral();
        state = 1569;
        match(TOKEN_COMMA);
        state = 1570;
        genericArgument();
        state = 1571;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalMethod_has_String_String_PContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 1573;
        match(TOKEN_K_HAS);
        state = 1574;
        match(TOKEN_LPAREN);
        state = 1575;
        stringNullableArgument();
        state = 1576;
        match(TOKEN_COMMA);
        state = 1577;
        stringNullableLiteral();
        state = 1578;
        match(TOKEN_COMMA);
        state = 1579;
        traversalPredicate(0);
        state = 1580;
        match(TOKEN_RPAREN);
        break;
      case 6:
        _localctx = TraversalMethod_has_T_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 6);
        state = 1582;
        match(TOKEN_K_HAS);
        state = 1583;
        match(TOKEN_LPAREN);
        state = 1584;
        traversalT();
        state = 1585;
        match(TOKEN_COMMA);
        state = 1586;
        genericArgument();
        state = 1587;
        match(TOKEN_RPAREN);
        break;
      case 7:
        _localctx = TraversalMethod_has_T_PContext(_localctx);
        enterOuterAlt(_localctx, 7);
        state = 1589;
        match(TOKEN_K_HAS);
        state = 1590;
        match(TOKEN_LPAREN);
        state = 1591;
        traversalT();
        state = 1592;
        match(TOKEN_COMMA);
        state = 1593;
        traversalPredicate(0);
        state = 1594;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasIdContext traversalMethod_hasId() {
    dynamic _localctx = TraversalMethod_hasIdContext(context, state);
    enterRule(_localctx, 158, RULE_traversalMethod_hasId);
    int _la;
    try {
      state = 1612;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 49, context)) {
      case 1:
        _localctx = TraversalMethod_hasId_Object_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1598;
        match(TOKEN_K_HASID);
        state = 1599;
        match(TOKEN_LPAREN);
        state = 1600;
        genericArgument();
        state = 1603;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1601;
          match(TOKEN_COMMA);
          state = 1602;
          genericArgumentVarargs();
        }

        state = 1605;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_hasId_PContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1607;
        match(TOKEN_K_HASID);
        state = 1608;
        match(TOKEN_LPAREN);
        state = 1609;
        traversalPredicate(0);
        state = 1610;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasKeyContext traversalMethod_hasKey() {
    dynamic _localctx = TraversalMethod_hasKeyContext(context, state);
    enterRule(_localctx, 160, RULE_traversalMethod_hasKey);
    int _la;
    try {
      state = 1628;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 51, context)) {
      case 1:
        _localctx = TraversalMethod_hasKey_PContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1614;
        match(TOKEN_K_HASKEY);
        state = 1615;
        match(TOKEN_LPAREN);
        state = 1616;
        traversalPredicate(0);
        state = 1617;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_hasKey_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1619;
        match(TOKEN_K_HASKEY);
        state = 1620;
        match(TOKEN_LPAREN);
        state = 1621;
        stringNullableLiteral();
        state = 1624;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1622;
          match(TOKEN_COMMA);
          state = 1623;
          stringNullableLiteralVarargs();
        }

        state = 1626;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasLabelContext traversalMethod_hasLabel() {
    dynamic _localctx = TraversalMethod_hasLabelContext(context, state);
    enterRule(_localctx, 162, RULE_traversalMethod_hasLabel);
    int _la;
    try {
      state = 1644;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 53, context)) {
      case 1:
        _localctx = TraversalMethod_hasLabel_PContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1630;
        match(TOKEN_K_HASLABEL);
        state = 1631;
        match(TOKEN_LPAREN);
        state = 1632;
        traversalPredicate(0);
        state = 1633;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_hasLabel_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1635;
        match(TOKEN_K_HASLABEL);
        state = 1636;
        match(TOKEN_LPAREN);
        state = 1637;
        stringNullableArgument();
        state = 1640;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1638;
          match(TOKEN_COMMA);
          state = 1639;
          stringNullableArgumentVarargs();
        }

        state = 1642;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasNotContext traversalMethod_hasNot() {
    dynamic _localctx = TraversalMethod_hasNotContext(context, state);
    enterRule(_localctx, 164, RULE_traversalMethod_hasNot);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1646;
      match(TOKEN_K_HASNOT);
      state = 1647;
      match(TOKEN_LPAREN);
      state = 1648;
      stringNullableLiteral();
      state = 1649;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_hasValueContext traversalMethod_hasValue() {
    dynamic _localctx = TraversalMethod_hasValueContext(context, state);
    enterRule(_localctx, 166, RULE_traversalMethod_hasValue);
    int _la;
    try {
      state = 1665;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 55, context)) {
      case 1:
        _localctx = TraversalMethod_hasValue_Object_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1651;
        match(TOKEN_K_HASVALUE);
        state = 1652;
        match(TOKEN_LPAREN);
        state = 1653;
        genericArgument();
        state = 1656;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1654;
          match(TOKEN_COMMA);
          state = 1655;
          genericArgumentVarargs();
        }

        state = 1658;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_hasValue_PContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1660;
        match(TOKEN_K_HASVALUE);
        state = 1661;
        match(TOKEN_LPAREN);
        state = 1662;
        traversalPredicate(0);
        state = 1663;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_idContext traversalMethod_id() {
    dynamic _localctx = TraversalMethod_idContext(context, state);
    enterRule(_localctx, 168, RULE_traversalMethod_id);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1667;
      match(TOKEN_K_ID);
      state = 1668;
      match(TOKEN_LPAREN);
      state = 1669;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_identityContext traversalMethod_identity() {
    dynamic _localctx = TraversalMethod_identityContext(context, state);
    enterRule(_localctx, 170, RULE_traversalMethod_identity);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1671;
      match(TOKEN_K_IDENTITY);
      state = 1672;
      match(TOKEN_LPAREN);
      state = 1673;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_inContext traversalMethod_in() {
    dynamic _localctx = TraversalMethod_inContext(context, state);
    enterRule(_localctx, 172, RULE_traversalMethod_in);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1675;
      match(TOKEN_K_IN);
      state = 1676;
      match(TOKEN_LPAREN);
      state = 1677;
      stringNullableArgumentVarargs();
      state = 1678;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_inEContext traversalMethod_inE() {
    dynamic _localctx = TraversalMethod_inEContext(context, state);
    enterRule(_localctx, 174, RULE_traversalMethod_inE);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1680;
      match(TOKEN_K_INE);
      state = 1681;
      match(TOKEN_LPAREN);
      state = 1682;
      stringNullableArgumentVarargs();
      state = 1683;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_intersectContext traversalMethod_intersect() {
    dynamic _localctx = TraversalMethod_intersectContext(context, state);
    enterRule(_localctx, 176, RULE_traversalMethod_intersect);
    try {
      _localctx = TraversalMethod_intersect_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1685;
      match(TOKEN_K_INTERSECT);
      state = 1686;
      match(TOKEN_LPAREN);
      state = 1687;
      genericLiteral();
      state = 1688;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_inVContext traversalMethod_inV() {
    dynamic _localctx = TraversalMethod_inVContext(context, state);
    enterRule(_localctx, 178, RULE_traversalMethod_inV);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1690;
      match(TOKEN_K_INV);
      state = 1691;
      match(TOKEN_LPAREN);
      state = 1692;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_indexContext traversalMethod_index() {
    dynamic _localctx = TraversalMethod_indexContext(context, state);
    enterRule(_localctx, 180, RULE_traversalMethod_index);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1694;
      match(TOKEN_K_INDEX);
      state = 1695;
      match(TOKEN_LPAREN);
      state = 1696;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_injectContext traversalMethod_inject() {
    dynamic _localctx = TraversalMethod_injectContext(context, state);
    enterRule(_localctx, 182, RULE_traversalMethod_inject);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1698;
      match(TOKEN_K_INJECT);
      state = 1699;
      match(TOKEN_LPAREN);
      state = 1700;
      genericLiteralVarargs();
      state = 1701;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_isContext traversalMethod_is() {
    dynamic _localctx = TraversalMethod_isContext(context, state);
    enterRule(_localctx, 184, RULE_traversalMethod_is);
    try {
      state = 1713;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 56, context)) {
      case 1:
        _localctx = TraversalMethod_is_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1703;
        match(TOKEN_K_IS);
        state = 1704;
        match(TOKEN_LPAREN);
        state = 1705;
        genericArgument();
        state = 1706;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_is_PContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1708;
        match(TOKEN_K_IS);
        state = 1709;
        match(TOKEN_LPAREN);
        state = 1710;
        traversalPredicate(0);
        state = 1711;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_keyContext traversalMethod_key() {
    dynamic _localctx = TraversalMethod_keyContext(context, state);
    enterRule(_localctx, 186, RULE_traversalMethod_key);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1715;
      match(TOKEN_K_KEY);
      state = 1716;
      match(TOKEN_LPAREN);
      state = 1717;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_labelContext traversalMethod_label() {
    dynamic _localctx = TraversalMethod_labelContext(context, state);
    enterRule(_localctx, 188, RULE_traversalMethod_label);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1719;
      match(TOKEN_K_LABEL);
      state = 1720;
      match(TOKEN_LPAREN);
      state = 1721;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_lengthContext traversalMethod_length() {
    dynamic _localctx = TraversalMethod_lengthContext(context, state);
    enterRule(_localctx, 190, RULE_traversalMethod_length);
    try {
      state = 1731;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 57, context)) {
      case 1:
        _localctx = TraversalMethod_length_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1723;
        match(TOKEN_K_LENGTH);
        state = 1724;
        match(TOKEN_LPAREN);
        state = 1725;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_length_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1726;
        match(TOKEN_K_LENGTH);
        state = 1727;
        match(TOKEN_LPAREN);
        state = 1728;
        traversalScope();
        state = 1729;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_limitContext traversalMethod_limit() {
    dynamic _localctx = TraversalMethod_limitContext(context, state);
    enterRule(_localctx, 192, RULE_traversalMethod_limit);
    try {
      state = 1745;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 58, context)) {
      case 1:
        _localctx = TraversalMethod_limit_Scope_longContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1733;
        match(TOKEN_K_LIMIT);
        state = 1734;
        match(TOKEN_LPAREN);
        state = 1735;
        traversalScope();
        state = 1736;
        match(TOKEN_COMMA);
        state = 1737;
        integerArgument();
        state = 1738;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_limit_longContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1740;
        match(TOKEN_K_LIMIT);
        state = 1741;
        match(TOKEN_LPAREN);
        state = 1742;
        integerArgument();
        state = 1743;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_localContext traversalMethod_local() {
    dynamic _localctx = TraversalMethod_localContext(context, state);
    enterRule(_localctx, 194, RULE_traversalMethod_local);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1747;
      match(TOKEN_K_LOCAL);
      state = 1748;
      match(TOKEN_LPAREN);
      state = 1749;
      nestedTraversal();
      state = 1750;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_loopsContext traversalMethod_loops() {
    dynamic _localctx = TraversalMethod_loopsContext(context, state);
    enterRule(_localctx, 196, RULE_traversalMethod_loops);
    try {
      state = 1760;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 59, context)) {
      case 1:
        _localctx = TraversalMethod_loops_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1752;
        match(TOKEN_K_LOOPS);
        state = 1753;
        match(TOKEN_LPAREN);
        state = 1754;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_loops_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1755;
        match(TOKEN_K_LOOPS);
        state = 1756;
        match(TOKEN_LPAREN);
        state = 1757;
        stringLiteral();
        state = 1758;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_lTrimContext traversalMethod_lTrim() {
    dynamic _localctx = TraversalMethod_lTrimContext(context, state);
    enterRule(_localctx, 198, RULE_traversalMethod_lTrim);
    try {
      state = 1770;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 60, context)) {
      case 1:
        _localctx = TraversalMethod_lTrim_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1762;
        match(TOKEN_K_LTRIM);
        state = 1763;
        match(TOKEN_LPAREN);
        state = 1764;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_lTrim_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1765;
        match(TOKEN_K_LTRIM);
        state = 1766;
        match(TOKEN_LPAREN);
        state = 1767;
        traversalScope();
        state = 1768;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_mapContext traversalMethod_map() {
    dynamic _localctx = TraversalMethod_mapContext(context, state);
    enterRule(_localctx, 200, RULE_traversalMethod_map);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1772;
      match(TOKEN_K_MAP);
      state = 1773;
      match(TOKEN_LPAREN);
      state = 1774;
      nestedTraversal();
      state = 1775;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_matchContext traversalMethod_match() {
    dynamic _localctx = TraversalMethod_matchContext(context, state);
    enterRule(_localctx, 202, RULE_traversalMethod_match);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1777;
      match(TOKEN_K_MATCH);
      state = 1778;
      match(TOKEN_LPAREN);
      state = 1779;
      nestedTraversalList();
      state = 1780;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_mathContext traversalMethod_math() {
    dynamic _localctx = TraversalMethod_mathContext(context, state);
    enterRule(_localctx, 204, RULE_traversalMethod_math);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1782;
      match(TOKEN_K_MATH);
      state = 1783;
      match(TOKEN_LPAREN);
      state = 1784;
      stringLiteral();
      state = 1785;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_maxContext traversalMethod_max() {
    dynamic _localctx = TraversalMethod_maxContext(context, state);
    enterRule(_localctx, 206, RULE_traversalMethod_max);
    try {
      state = 1795;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 61, context)) {
      case 1:
        _localctx = TraversalMethod_max_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1787;
        match(TOKEN_K_MAX);
        state = 1788;
        match(TOKEN_LPAREN);
        state = 1789;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_max_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1790;
        match(TOKEN_K_MAX);
        state = 1791;
        match(TOKEN_LPAREN);
        state = 1792;
        traversalScope();
        state = 1793;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_meanContext traversalMethod_mean() {
    dynamic _localctx = TraversalMethod_meanContext(context, state);
    enterRule(_localctx, 208, RULE_traversalMethod_mean);
    try {
      state = 1805;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 62, context)) {
      case 1:
        _localctx = TraversalMethod_mean_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1797;
        match(TOKEN_K_MEAN);
        state = 1798;
        match(TOKEN_LPAREN);
        state = 1799;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_mean_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1800;
        match(TOKEN_K_MEAN);
        state = 1801;
        match(TOKEN_LPAREN);
        state = 1802;
        traversalScope();
        state = 1803;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_mergeContext traversalMethod_merge() {
    dynamic _localctx = TraversalMethod_mergeContext(context, state);
    enterRule(_localctx, 210, RULE_traversalMethod_merge);
    try {
      _localctx = TraversalMethod_merge_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1807;
      match(TOKEN_K_MERGE);
      state = 1808;
      match(TOKEN_LPAREN);
      state = 1809;
      genericLiteral();
      state = 1810;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_mergeVContext traversalMethod_mergeV() {
    dynamic _localctx = TraversalMethod_mergeVContext(context, state);
    enterRule(_localctx, 212, RULE_traversalMethod_mergeV);
    try {
      state = 1825;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 63, context)) {
      case 1:
        _localctx = TraversalMethod_mergeV_emptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1812;
        match(TOKEN_K_MERGEV);
        state = 1813;
        match(TOKEN_LPAREN);
        state = 1814;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_mergeV_MapContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1815;
        match(TOKEN_K_MERGEV);
        state = 1816;
        match(TOKEN_LPAREN);
        state = 1817;
        genericMapNullableArgument();
        state = 1818;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_mergeV_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1820;
        match(TOKEN_K_MERGEV);
        state = 1821;
        match(TOKEN_LPAREN);
        state = 1822;
        nestedTraversal();
        state = 1823;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_mergeEContext traversalMethod_mergeE() {
    dynamic _localctx = TraversalMethod_mergeEContext(context, state);
    enterRule(_localctx, 214, RULE_traversalMethod_mergeE);
    try {
      state = 1840;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 64, context)) {
      case 1:
        _localctx = TraversalMethod_mergeE_emptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1827;
        match(TOKEN_K_MERGEE);
        state = 1828;
        match(TOKEN_LPAREN);
        state = 1829;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_mergeE_MapContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1830;
        match(TOKEN_K_MERGEE);
        state = 1831;
        match(TOKEN_LPAREN);
        state = 1832;
        genericMapNullableArgument();
        state = 1833;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_mergeE_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1835;
        match(TOKEN_K_MERGEE);
        state = 1836;
        match(TOKEN_LPAREN);
        state = 1837;
        nestedTraversal();
        state = 1838;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_minContext traversalMethod_min() {
    dynamic _localctx = TraversalMethod_minContext(context, state);
    enterRule(_localctx, 216, RULE_traversalMethod_min);
    try {
      state = 1850;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 65, context)) {
      case 1:
        _localctx = TraversalMethod_min_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1842;
        match(TOKEN_K_MIN);
        state = 1843;
        match(TOKEN_LPAREN);
        state = 1844;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_min_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1845;
        match(TOKEN_K_MIN);
        state = 1846;
        match(TOKEN_LPAREN);
        state = 1847;
        traversalScope();
        state = 1848;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_noneContext traversalMethod_none() {
    dynamic _localctx = TraversalMethod_noneContext(context, state);
    enterRule(_localctx, 218, RULE_traversalMethod_none);
    try {
      _localctx = TraversalMethod_none_PContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1852;
      match(TOKEN_K_NONE);
      state = 1853;
      match(TOKEN_LPAREN);
      state = 1854;
      traversalPredicate(0);
      state = 1855;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_notContext traversalMethod_not() {
    dynamic _localctx = TraversalMethod_notContext(context, state);
    enterRule(_localctx, 220, RULE_traversalMethod_not);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1857;
      match(TOKEN_K_NOT);
      state = 1858;
      match(TOKEN_LPAREN);
      state = 1859;
      nestedTraversal();
      state = 1860;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_optionContext traversalMethod_option() {
    dynamic _localctx = TraversalMethod_optionContext(context, state);
    enterRule(_localctx, 222, RULE_traversalMethod_option);
    try {
      state = 1904;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 66, context)) {
      case 1:
        _localctx = TraversalMethod_option_Predicate_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1862;
        match(TOKEN_K_OPTION);
        state = 1863;
        match(TOKEN_LPAREN);
        state = 1864;
        traversalPredicate(0);
        state = 1865;
        match(TOKEN_COMMA);
        state = 1866;
        nestedTraversal();
        state = 1867;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_option_Merge_MapContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1869;
        match(TOKEN_K_OPTION);
        state = 1870;
        match(TOKEN_LPAREN);
        state = 1871;
        traversalMerge();
        state = 1872;
        match(TOKEN_COMMA);
        state = 1873;
        genericMapNullableArgument();
        state = 1874;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_option_Merge_Map_CardinalityContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 1876;
        match(TOKEN_K_OPTION);
        state = 1877;
        match(TOKEN_LPAREN);
        state = 1878;
        traversalMerge();
        state = 1879;
        match(TOKEN_COMMA);
        state = 1880;
        genericMapNullableArgument();
        state = 1881;
        match(TOKEN_COMMA);
        state = 1882;
        traversalCardinality();
        state = 1883;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_option_Merge_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 1885;
        match(TOKEN_K_OPTION);
        state = 1886;
        match(TOKEN_LPAREN);
        state = 1887;
        traversalMerge();
        state = 1888;
        match(TOKEN_COMMA);
        state = 1889;
        nestedTraversal();
        state = 1890;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalMethod_option_Object_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 1892;
        match(TOKEN_K_OPTION);
        state = 1893;
        match(TOKEN_LPAREN);
        state = 1894;
        genericArgument();
        state = 1895;
        match(TOKEN_COMMA);
        state = 1896;
        nestedTraversal();
        state = 1897;
        match(TOKEN_RPAREN);
        break;
      case 6:
        _localctx = TraversalMethod_option_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 6);
        state = 1899;
        match(TOKEN_K_OPTION);
        state = 1900;
        match(TOKEN_LPAREN);
        state = 1901;
        nestedTraversal();
        state = 1902;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_optionalContext traversalMethod_optional() {
    dynamic _localctx = TraversalMethod_optionalContext(context, state);
    enterRule(_localctx, 224, RULE_traversalMethod_optional);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1906;
      match(TOKEN_K_OPTIONAL);
      state = 1907;
      match(TOKEN_LPAREN);
      state = 1908;
      nestedTraversal();
      state = 1909;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_orContext traversalMethod_or() {
    dynamic _localctx = TraversalMethod_orContext(context, state);
    enterRule(_localctx, 226, RULE_traversalMethod_or);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1911;
      match(TOKEN_K_OR);
      state = 1912;
      match(TOKEN_LPAREN);
      state = 1913;
      nestedTraversalList();
      state = 1914;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_orderContext traversalMethod_order() {
    dynamic _localctx = TraversalMethod_orderContext(context, state);
    enterRule(_localctx, 228, RULE_traversalMethod_order);
    try {
      state = 1924;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 67, context)) {
      case 1:
        _localctx = TraversalMethod_order_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1916;
        match(TOKEN_K_ORDER);
        state = 1917;
        match(TOKEN_LPAREN);
        state = 1918;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_order_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1919;
        match(TOKEN_K_ORDER);
        state = 1920;
        match(TOKEN_LPAREN);
        state = 1921;
        traversalScope();
        state = 1922;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_otherVContext traversalMethod_otherV() {
    dynamic _localctx = TraversalMethod_otherVContext(context, state);
    enterRule(_localctx, 230, RULE_traversalMethod_otherV);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1926;
      match(TOKEN_K_OTHERV);
      state = 1927;
      match(TOKEN_LPAREN);
      state = 1928;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_outContext traversalMethod_out() {
    dynamic _localctx = TraversalMethod_outContext(context, state);
    enterRule(_localctx, 232, RULE_traversalMethod_out);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1930;
      match(TOKEN_K_OUT);
      state = 1931;
      match(TOKEN_LPAREN);
      state = 1932;
      stringNullableArgumentVarargs();
      state = 1933;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_outEContext traversalMethod_outE() {
    dynamic _localctx = TraversalMethod_outEContext(context, state);
    enterRule(_localctx, 234, RULE_traversalMethod_outE);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1935;
      match(TOKEN_K_OUTE);
      state = 1936;
      match(TOKEN_LPAREN);
      state = 1937;
      stringNullableArgumentVarargs();
      state = 1938;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_outVContext traversalMethod_outV() {
    dynamic _localctx = TraversalMethod_outVContext(context, state);
    enterRule(_localctx, 236, RULE_traversalMethod_outV);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1940;
      match(TOKEN_K_OUTV);
      state = 1941;
      match(TOKEN_LPAREN);
      state = 1942;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_pageRankContext traversalMethod_pageRank() {
    dynamic _localctx = TraversalMethod_pageRankContext(context, state);
    enterRule(_localctx, 238, RULE_traversalMethod_pageRank);
    try {
      state = 1952;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 68, context)) {
      case 1:
        _localctx = TraversalMethod_pageRank_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1944;
        match(TOKEN_K_PAGERANK);
        state = 1945;
        match(TOKEN_LPAREN);
        state = 1946;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_pageRank_doubleContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1947;
        match(TOKEN_K_PAGERANK);
        state = 1948;
        match(TOKEN_LPAREN);
        state = 1949;
        numericLiteral();
        state = 1950;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_pathContext traversalMethod_path() {
    dynamic _localctx = TraversalMethod_pathContext(context, state);
    enterRule(_localctx, 240, RULE_traversalMethod_path);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1954;
      match(TOKEN_K_PATH);
      state = 1955;
      match(TOKEN_LPAREN);
      state = 1956;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_peerPressureContext traversalMethod_peerPressure() {
    dynamic _localctx = TraversalMethod_peerPressureContext(context, state);
    enterRule(_localctx, 242, RULE_traversalMethod_peerPressure);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1958;
      match(TOKEN_K_PEERPRESSURE);
      state = 1959;
      match(TOKEN_LPAREN);
      state = 1960;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_productContext traversalMethod_product() {
    dynamic _localctx = TraversalMethod_productContext(context, state);
    enterRule(_localctx, 244, RULE_traversalMethod_product);
    try {
      _localctx = TraversalMethod_product_ObjectContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 1962;
      match(TOKEN_K_PRODUCT);
      state = 1963;
      match(TOKEN_LPAREN);
      state = 1964;
      genericLiteral();
      state = 1965;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_profileContext traversalMethod_profile() {
    dynamic _localctx = TraversalMethod_profileContext(context, state);
    enterRule(_localctx, 246, RULE_traversalMethod_profile);
    try {
      state = 1975;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 69, context)) {
      case 1:
        _localctx = TraversalMethod_profile_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1967;
        match(TOKEN_K_PROFILE);
        state = 1968;
        match(TOKEN_LPAREN);
        state = 1969;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_profile_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 1970;
        match(TOKEN_K_PROFILE);
        state = 1971;
        match(TOKEN_LPAREN);
        state = 1972;
        stringLiteral();
        state = 1973;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_projectContext traversalMethod_project() {
    dynamic _localctx = TraversalMethod_projectContext(context, state);
    enterRule(_localctx, 248, RULE_traversalMethod_project);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 1977;
      match(TOKEN_K_PROJECT);
      state = 1978;
      match(TOKEN_LPAREN);
      state = 1979;
      stringLiteral();
      state = 1982;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 1980;
        match(TOKEN_COMMA);
        state = 1981;
        stringNullableLiteralVarargs();
      }

      state = 1984;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_propertiesContext traversalMethod_properties() {
    dynamic _localctx = TraversalMethod_propertiesContext(context, state);
    enterRule(_localctx, 250, RULE_traversalMethod_properties);
    try {
      enterOuterAlt(_localctx, 1);
      state = 1986;
      match(TOKEN_K_PROPERTIES);
      state = 1987;
      match(TOKEN_LPAREN);
      state = 1988;
      stringNullableLiteralVarargs();
      state = 1989;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_propertyContext traversalMethod_property() {
    dynamic _localctx = TraversalMethod_propertyContext(context, state);
    enterRule(_localctx, 252, RULE_traversalMethod_property);
    int _la;
    try {
      state = 2027;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 73, context)) {
      case 1:
        _localctx = TraversalMethod_property_Cardinality_Object_Object_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 1991;
        match(TOKEN_K_PROPERTY);
        state = 1992;
        match(TOKEN_LPAREN);
        state = 1993;
        traversalCardinality();
        state = 1994;
        match(TOKEN_COMMA);
        state = 1995;
        genericLiteral();
        state = 1996;
        match(TOKEN_COMMA);
        state = 1997;
        genericArgument();
        state = 2000;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 1998;
          match(TOKEN_COMMA);
          state = 1999;
          genericArgumentVarargs();
        }

        state = 2002;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_property_Cardinality_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2004;
        match(TOKEN_K_PROPERTY);
        state = 2005;
        match(TOKEN_LPAREN);
        state = 2006;
        traversalCardinality();
        state = 2007;
        match(TOKEN_COMMA);
        state = 2008;
        genericMapNullableArgument();
        state = 2009;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_property_Object_Object_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2011;
        match(TOKEN_K_PROPERTY);
        state = 2012;
        match(TOKEN_LPAREN);
        state = 2013;
        genericLiteral();
        state = 2014;
        match(TOKEN_COMMA);
        state = 2015;
        genericArgument();
        state = 2018;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 2016;
          match(TOKEN_COMMA);
          state = 2017;
          genericArgumentVarargs();
        }

        state = 2020;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_property_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 2022;
        match(TOKEN_K_PROPERTY);
        state = 2023;
        match(TOKEN_LPAREN);
        state = 2024;
        genericMapNullableArgument();
        state = 2025;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_propertyMapContext traversalMethod_propertyMap() {
    dynamic _localctx = TraversalMethod_propertyMapContext(context, state);
    enterRule(_localctx, 254, RULE_traversalMethod_propertyMap);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2029;
      match(TOKEN_K_PROPERTYMAP);
      state = 2030;
      match(TOKEN_LPAREN);
      state = 2031;
      stringNullableLiteralVarargs();
      state = 2032;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_rangeContext traversalMethod_range() {
    dynamic _localctx = TraversalMethod_rangeContext(context, state);
    enterRule(_localctx, 256, RULE_traversalMethod_range);
    try {
      state = 2050;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 74, context)) {
      case 1:
        _localctx = TraversalMethod_range_Scope_long_longContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2034;
        match(TOKEN_K_RANGE);
        state = 2035;
        match(TOKEN_LPAREN);
        state = 2036;
        traversalScope();
        state = 2037;
        match(TOKEN_COMMA);
        state = 2038;
        integerArgument();
        state = 2039;
        match(TOKEN_COMMA);
        state = 2040;
        integerArgument();
        state = 2041;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_range_long_longContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2043;
        match(TOKEN_K_RANGE);
        state = 2044;
        match(TOKEN_LPAREN);
        state = 2045;
        integerArgument();
        state = 2046;
        match(TOKEN_COMMA);
        state = 2047;
        integerArgument();
        state = 2048;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_readContext traversalMethod_read() {
    dynamic _localctx = TraversalMethod_readContext(context, state);
    enterRule(_localctx, 258, RULE_traversalMethod_read);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2052;
      match(TOKEN_K_READ);
      state = 2053;
      match(TOKEN_LPAREN);
      state = 2054;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_repeatContext traversalMethod_repeat() {
    dynamic _localctx = TraversalMethod_repeatContext(context, state);
    enterRule(_localctx, 260, RULE_traversalMethod_repeat);
    try {
      state = 2068;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 75, context)) {
      case 1:
        _localctx = TraversalMethod_repeat_String_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2056;
        match(TOKEN_K_REPEAT);
        state = 2057;
        match(TOKEN_LPAREN);
        state = 2058;
        stringLiteral();
        state = 2059;
        match(TOKEN_COMMA);
        state = 2060;
        nestedTraversal();
        state = 2061;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_repeat_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2063;
        match(TOKEN_K_REPEAT);
        state = 2064;
        match(TOKEN_LPAREN);
        state = 2065;
        nestedTraversal();
        state = 2066;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_replaceContext traversalMethod_replace() {
    dynamic _localctx = TraversalMethod_replaceContext(context, state);
    enterRule(_localctx, 262, RULE_traversalMethod_replace);
    try {
      state = 2086;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 76, context)) {
      case 1:
        _localctx = TraversalMethod_replace_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2070;
        match(TOKEN_K_REPLACE);
        state = 2071;
        match(TOKEN_LPAREN);
        state = 2072;
        stringNullableLiteral();
        state = 2073;
        match(TOKEN_COMMA);
        state = 2074;
        stringNullableLiteral();
        state = 2075;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_replace_Scope_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2077;
        match(TOKEN_K_REPLACE);
        state = 2078;
        match(TOKEN_LPAREN);
        state = 2079;
        traversalScope();
        state = 2080;
        match(TOKEN_COMMA);
        state = 2081;
        stringNullableLiteral();
        state = 2082;
        match(TOKEN_COMMA);
        state = 2083;
        stringNullableLiteral();
        state = 2084;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_reverseContext traversalMethod_reverse() {
    dynamic _localctx = TraversalMethod_reverseContext(context, state);
    enterRule(_localctx, 264, RULE_traversalMethod_reverse);
    try {
      _localctx = TraversalMethod_reverse_EmptyContext(_localctx);
      enterOuterAlt(_localctx, 1);
      state = 2088;
      match(TOKEN_K_REVERSE);
      state = 2089;
      match(TOKEN_LPAREN);
      state = 2090;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_rTrimContext traversalMethod_rTrim() {
    dynamic _localctx = TraversalMethod_rTrimContext(context, state);
    enterRule(_localctx, 266, RULE_traversalMethod_rTrim);
    try {
      state = 2100;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 77, context)) {
      case 1:
        _localctx = TraversalMethod_rTrim_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2092;
        match(TOKEN_K_RTRIM);
        state = 2093;
        match(TOKEN_LPAREN);
        state = 2094;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_rTrim_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2095;
        match(TOKEN_K_RTRIM);
        state = 2096;
        match(TOKEN_LPAREN);
        state = 2097;
        traversalScope();
        state = 2098;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_sackContext traversalMethod_sack() {
    dynamic _localctx = TraversalMethod_sackContext(context, state);
    enterRule(_localctx, 268, RULE_traversalMethod_sack);
    try {
      state = 2110;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 78, context)) {
      case 1:
        _localctx = TraversalMethod_sack_BiFunctionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2102;
        match(TOKEN_K_SACK);
        state = 2103;
        match(TOKEN_LPAREN);
        state = 2104;
        traversalBiFunction();
        state = 2105;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_sack_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2107;
        match(TOKEN_K_SACK);
        state = 2108;
        match(TOKEN_LPAREN);
        state = 2109;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_sampleContext traversalMethod_sample() {
    dynamic _localctx = TraversalMethod_sampleContext(context, state);
    enterRule(_localctx, 270, RULE_traversalMethod_sample);
    try {
      state = 2124;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 79, context)) {
      case 1:
        _localctx = TraversalMethod_sample_Scope_intContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2112;
        match(TOKEN_K_SAMPLE);
        state = 2113;
        match(TOKEN_LPAREN);
        state = 2114;
        traversalScope();
        state = 2115;
        match(TOKEN_COMMA);
        state = 2116;
        integerLiteral();
        state = 2117;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_sample_intContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2119;
        match(TOKEN_K_SAMPLE);
        state = 2120;
        match(TOKEN_LPAREN);
        state = 2121;
        integerLiteral();
        state = 2122;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_selectContext traversalMethod_select() {
    dynamic _localctx = TraversalMethod_selectContext(context, state);
    enterRule(_localctx, 272, RULE_traversalMethod_select);
    int _la;
    try {
      state = 2179;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 82, context)) {
      case 1:
        _localctx = TraversalMethod_select_ColumnContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2126;
        match(TOKEN_K_SELECT);
        state = 2127;
        match(TOKEN_LPAREN);
        state = 2128;
        traversalColumn();
        state = 2129;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_select_Pop_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2131;
        match(TOKEN_K_SELECT);
        state = 2132;
        match(TOKEN_LPAREN);
        state = 2133;
        traversalPop();
        state = 2134;
        match(TOKEN_COMMA);
        state = 2135;
        stringLiteral();
        state = 2136;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_select_Pop_String_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2138;
        match(TOKEN_K_SELECT);
        state = 2139;
        match(TOKEN_LPAREN);
        state = 2140;
        traversalPop();
        state = 2141;
        match(TOKEN_COMMA);
        state = 2142;
        stringLiteral();
        state = 2143;
        match(TOKEN_COMMA);
        state = 2144;
        stringLiteral();
        state = 2147;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 2145;
          match(TOKEN_COMMA);
          state = 2146;
          stringNullableLiteralVarargs();
        }

        state = 2149;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_select_Pop_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 2151;
        match(TOKEN_K_SELECT);
        state = 2152;
        match(TOKEN_LPAREN);
        state = 2153;
        traversalPop();
        state = 2154;
        match(TOKEN_COMMA);
        state = 2155;
        nestedTraversal();
        state = 2156;
        match(TOKEN_RPAREN);
        break;
      case 5:
        _localctx = TraversalMethod_select_StringContext(_localctx);
        enterOuterAlt(_localctx, 5);
        state = 2158;
        match(TOKEN_K_SELECT);
        state = 2159;
        match(TOKEN_LPAREN);
        state = 2160;
        stringLiteral();
        state = 2161;
        match(TOKEN_RPAREN);
        break;
      case 6:
        _localctx = TraversalMethod_select_String_String_StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        state = 2163;
        match(TOKEN_K_SELECT);
        state = 2164;
        match(TOKEN_LPAREN);
        state = 2165;
        stringLiteral();
        state = 2166;
        match(TOKEN_COMMA);
        state = 2167;
        stringLiteral();
        state = 2170;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 2168;
          match(TOKEN_COMMA);
          state = 2169;
          stringNullableLiteralVarargs();
        }

        state = 2172;
        match(TOKEN_RPAREN);
        break;
      case 7:
        _localctx = TraversalMethod_select_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 7);
        state = 2174;
        match(TOKEN_K_SELECT);
        state = 2175;
        match(TOKEN_LPAREN);
        state = 2176;
        nestedTraversal();
        state = 2177;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_shortestPathContext traversalMethod_shortestPath() {
    dynamic _localctx = TraversalMethod_shortestPathContext(context, state);
    enterRule(_localctx, 274, RULE_traversalMethod_shortestPath);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2181;
      match(TOKEN_K_SHORTESTPATH);
      state = 2182;
      match(TOKEN_LPAREN);
      state = 2183;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_sideEffectContext traversalMethod_sideEffect() {
    dynamic _localctx = TraversalMethod_sideEffectContext(context, state);
    enterRule(_localctx, 276, RULE_traversalMethod_sideEffect);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2185;
      match(TOKEN_K_SIDEEFFECT);
      state = 2186;
      match(TOKEN_LPAREN);
      state = 2187;
      nestedTraversal();
      state = 2188;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_simplePathContext traversalMethod_simplePath() {
    dynamic _localctx = TraversalMethod_simplePathContext(context, state);
    enterRule(_localctx, 278, RULE_traversalMethod_simplePath);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2190;
      match(TOKEN_K_SIMPLEPATH);
      state = 2191;
      match(TOKEN_LPAREN);
      state = 2192;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_skipContext traversalMethod_skip() {
    dynamic _localctx = TraversalMethod_skipContext(context, state);
    enterRule(_localctx, 280, RULE_traversalMethod_skip);
    try {
      state = 2206;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 83, context)) {
      case 1:
        _localctx = TraversalMethod_skip_Scope_longContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2194;
        match(TOKEN_K_SKIP);
        state = 2195;
        match(TOKEN_LPAREN);
        state = 2196;
        traversalScope();
        state = 2197;
        match(TOKEN_COMMA);
        state = 2198;
        integerArgument();
        state = 2199;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_skip_longContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2201;
        match(TOKEN_K_SKIP);
        state = 2202;
        match(TOKEN_LPAREN);
        state = 2203;
        integerArgument();
        state = 2204;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_splitContext traversalMethod_split() {
    dynamic _localctx = TraversalMethod_splitContext(context, state);
    enterRule(_localctx, 282, RULE_traversalMethod_split);
    try {
      state = 2220;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 84, context)) {
      case 1:
        _localctx = TraversalMethod_split_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2208;
        match(TOKEN_K_SPLIT);
        state = 2209;
        match(TOKEN_LPAREN);
        state = 2210;
        stringNullableLiteral();
        state = 2211;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_split_Scope_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2213;
        match(TOKEN_K_SPLIT);
        state = 2214;
        match(TOKEN_LPAREN);
        state = 2215;
        traversalScope();
        state = 2216;
        match(TOKEN_COMMA);
        state = 2217;
        stringNullableLiteral();
        state = 2218;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_subgraphContext traversalMethod_subgraph() {
    dynamic _localctx = TraversalMethod_subgraphContext(context, state);
    enterRule(_localctx, 284, RULE_traversalMethod_subgraph);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2222;
      match(TOKEN_K_SUBGRAPH);
      state = 2223;
      match(TOKEN_LPAREN);
      state = 2224;
      stringLiteral();
      state = 2225;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_substringContext traversalMethod_substring() {
    dynamic _localctx = TraversalMethod_substringContext(context, state);
    enterRule(_localctx, 286, RULE_traversalMethod_substring);
    try {
      state = 2255;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 85, context)) {
      case 1:
        _localctx = TraversalMethod_substring_intContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2227;
        match(TOKEN_K_SUBSTRING);
        state = 2228;
        match(TOKEN_LPAREN);
        state = 2229;
        integerLiteral();
        state = 2230;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_substring_Scope_intContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2232;
        match(TOKEN_K_SUBSTRING);
        state = 2233;
        match(TOKEN_LPAREN);
        state = 2234;
        traversalScope();
        state = 2235;
        match(TOKEN_COMMA);
        state = 2236;
        integerLiteral();
        state = 2237;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_substring_int_intContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2239;
        match(TOKEN_K_SUBSTRING);
        state = 2240;
        match(TOKEN_LPAREN);
        state = 2241;
        integerLiteral();
        state = 2242;
        match(TOKEN_COMMA);
        state = 2243;
        integerLiteral();
        state = 2244;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_substring_Scope_int_intContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 2246;
        match(TOKEN_K_SUBSTRING);
        state = 2247;
        match(TOKEN_LPAREN);
        state = 2248;
        traversalScope();
        state = 2249;
        match(TOKEN_COMMA);
        state = 2250;
        integerLiteral();
        state = 2251;
        match(TOKEN_COMMA);
        state = 2252;
        integerLiteral();
        state = 2253;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_sumContext traversalMethod_sum() {
    dynamic _localctx = TraversalMethod_sumContext(context, state);
    enterRule(_localctx, 288, RULE_traversalMethod_sum);
    try {
      state = 2265;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 86, context)) {
      case 1:
        _localctx = TraversalMethod_sum_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2257;
        match(TOKEN_K_SUM);
        state = 2258;
        match(TOKEN_LPAREN);
        state = 2259;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_sum_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2260;
        match(TOKEN_K_SUM);
        state = 2261;
        match(TOKEN_LPAREN);
        state = 2262;
        traversalScope();
        state = 2263;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_tailContext traversalMethod_tail() {
    dynamic _localctx = TraversalMethod_tailContext(context, state);
    enterRule(_localctx, 290, RULE_traversalMethod_tail);
    try {
      state = 2287;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 87, context)) {
      case 1:
        _localctx = TraversalMethod_tail_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2267;
        match(TOKEN_K_TAIL);
        state = 2268;
        match(TOKEN_LPAREN);
        state = 2269;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_tail_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2270;
        match(TOKEN_K_TAIL);
        state = 2271;
        match(TOKEN_LPAREN);
        state = 2272;
        traversalScope();
        state = 2273;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_tail_Scope_longContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2275;
        match(TOKEN_K_TAIL);
        state = 2276;
        match(TOKEN_LPAREN);
        state = 2277;
        traversalScope();
        state = 2278;
        match(TOKEN_COMMA);
        state = 2279;
        integerArgument();
        state = 2280;
        match(TOKEN_RPAREN);
        break;
      case 4:
        _localctx = TraversalMethod_tail_longContext(_localctx);
        enterOuterAlt(_localctx, 4);
        state = 2282;
        match(TOKEN_K_TAIL);
        state = 2283;
        match(TOKEN_LPAREN);
        state = 2284;
        integerArgument();
        state = 2285;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_timeLimitContext traversalMethod_timeLimit() {
    dynamic _localctx = TraversalMethod_timeLimitContext(context, state);
    enterRule(_localctx, 292, RULE_traversalMethod_timeLimit);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2289;
      match(TOKEN_K_TIMELIMIT);
      state = 2290;
      match(TOKEN_LPAREN);
      state = 2291;
      integerLiteral();
      state = 2292;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_timesContext traversalMethod_times() {
    dynamic _localctx = TraversalMethod_timesContext(context, state);
    enterRule(_localctx, 294, RULE_traversalMethod_times);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2294;
      match(TOKEN_K_TIMES);
      state = 2295;
      match(TOKEN_LPAREN);
      state = 2296;
      integerLiteral();
      state = 2297;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_toContext traversalMethod_to() {
    dynamic _localctx = TraversalMethod_toContext(context, state);
    enterRule(_localctx, 296, RULE_traversalMethod_to);
    int _la;
    try {
      state = 2318;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 89, context)) {
      case 1:
        _localctx = TraversalMethod_to_Direction_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2299;
        match(TOKEN_K_TO);
        state = 2300;
        match(TOKEN_LPAREN);
        state = 2301;
        traversalDirection();
        state = 2304;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 2302;
          match(TOKEN_COMMA);
          state = 2303;
          stringNullableArgumentVarargs();
        }

        state = 2306;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_to_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2308;
        match(TOKEN_K_TO);
        state = 2309;
        match(TOKEN_LPAREN);
        state = 2310;
        stringLiteral();
        state = 2311;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_to_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2313;
        match(TOKEN_K_TO);
        state = 2314;
        match(TOKEN_LPAREN);
        state = 2315;
        nestedTraversal();
        state = 2316;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_toEContext traversalMethod_toE() {
    dynamic _localctx = TraversalMethod_toEContext(context, state);
    enterRule(_localctx, 298, RULE_traversalMethod_toE);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 2320;
      match(TOKEN_K_TOE);
      state = 2321;
      match(TOKEN_LPAREN);
      state = 2322;
      traversalDirection();
      state = 2325;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 2323;
        match(TOKEN_COMMA);
        state = 2324;
        stringNullableArgumentVarargs();
      }

      state = 2327;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_toLowerContext traversalMethod_toLower() {
    dynamic _localctx = TraversalMethod_toLowerContext(context, state);
    enterRule(_localctx, 300, RULE_traversalMethod_toLower);
    try {
      state = 2337;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 91, context)) {
      case 1:
        _localctx = TraversalMethod_toLower_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2329;
        match(TOKEN_K_TOLOWER);
        state = 2330;
        match(TOKEN_LPAREN);
        state = 2331;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_toLower_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2332;
        match(TOKEN_K_TOLOWER);
        state = 2333;
        match(TOKEN_LPAREN);
        state = 2334;
        traversalScope();
        state = 2335;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_toUpperContext traversalMethod_toUpper() {
    dynamic _localctx = TraversalMethod_toUpperContext(context, state);
    enterRule(_localctx, 302, RULE_traversalMethod_toUpper);
    try {
      state = 2347;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 92, context)) {
      case 1:
        _localctx = TraversalMethod_toUpper_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2339;
        match(TOKEN_K_TOUPPER);
        state = 2340;
        match(TOKEN_LPAREN);
        state = 2341;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_toUpper_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2342;
        match(TOKEN_K_TOUPPER);
        state = 2343;
        match(TOKEN_LPAREN);
        state = 2344;
        traversalScope();
        state = 2345;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_toVContext traversalMethod_toV() {
    dynamic _localctx = TraversalMethod_toVContext(context, state);
    enterRule(_localctx, 304, RULE_traversalMethod_toV);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2349;
      match(TOKEN_K_TOV);
      state = 2350;
      match(TOKEN_LPAREN);
      state = 2351;
      traversalDirection();
      state = 2352;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_treeContext traversalMethod_tree() {
    dynamic _localctx = TraversalMethod_treeContext(context, state);
    enterRule(_localctx, 306, RULE_traversalMethod_tree);
    try {
      state = 2362;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 93, context)) {
      case 1:
        _localctx = TraversalMethod_tree_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2354;
        match(TOKEN_K_TREE);
        state = 2355;
        match(TOKEN_LPAREN);
        state = 2356;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_tree_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2357;
        match(TOKEN_K_TREE);
        state = 2358;
        match(TOKEN_LPAREN);
        state = 2359;
        stringLiteral();
        state = 2360;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_trimContext traversalMethod_trim() {
    dynamic _localctx = TraversalMethod_trimContext(context, state);
    enterRule(_localctx, 308, RULE_traversalMethod_trim);
    try {
      state = 2372;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 94, context)) {
      case 1:
        _localctx = TraversalMethod_trim_EmptyContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2364;
        match(TOKEN_K_TRIM);
        state = 2365;
        match(TOKEN_LPAREN);
        state = 2366;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_trim_ScopeContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2367;
        match(TOKEN_K_TRIM);
        state = 2368;
        match(TOKEN_LPAREN);
        state = 2369;
        traversalScope();
        state = 2370;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_unfoldContext traversalMethod_unfold() {
    dynamic _localctx = TraversalMethod_unfoldContext(context, state);
    enterRule(_localctx, 310, RULE_traversalMethod_unfold);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2374;
      match(TOKEN_K_UNFOLD);
      state = 2375;
      match(TOKEN_LPAREN);
      state = 2376;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_unionContext traversalMethod_union() {
    dynamic _localctx = TraversalMethod_unionContext(context, state);
    enterRule(_localctx, 312, RULE_traversalMethod_union);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2378;
      match(TOKEN_K_UNION);
      state = 2379;
      match(TOKEN_LPAREN);
      state = 2380;
      nestedTraversalList();
      state = 2381;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_untilContext traversalMethod_until() {
    dynamic _localctx = TraversalMethod_untilContext(context, state);
    enterRule(_localctx, 314, RULE_traversalMethod_until);
    try {
      state = 2393;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 95, context)) {
      case 1:
        _localctx = TraversalMethod_until_PredicateContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2383;
        match(TOKEN_K_UNTIL);
        state = 2384;
        match(TOKEN_LPAREN);
        state = 2385;
        traversalPredicate(0);
        state = 2386;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_until_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2388;
        match(TOKEN_K_UNTIL);
        state = 2389;
        match(TOKEN_LPAREN);
        state = 2390;
        nestedTraversal();
        state = 2391;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_valueContext traversalMethod_value() {
    dynamic _localctx = TraversalMethod_valueContext(context, state);
    enterRule(_localctx, 316, RULE_traversalMethod_value);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2395;
      match(TOKEN_K_VALUE);
      state = 2396;
      match(TOKEN_LPAREN);
      state = 2397;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_valueMapContext traversalMethod_valueMap() {
    dynamic _localctx = TraversalMethod_valueMapContext(context, state);
    enterRule(_localctx, 318, RULE_traversalMethod_valueMap);
    int _la;
    try {
      state = 2413;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 97, context)) {
      case 1:
        _localctx = TraversalMethod_valueMap_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2399;
        match(TOKEN_K_VALUEMAP);
        state = 2400;
        match(TOKEN_LPAREN);
        state = 2401;
        stringNullableLiteralVarargs();
        state = 2402;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_valueMap_boolean_StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2404;
        match(TOKEN_K_VALUEMAP);
        state = 2405;
        match(TOKEN_LPAREN);
        state = 2406;
        booleanLiteral();
        state = 2409;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if (_la == TOKEN_COMMA) {
          state = 2407;
          match(TOKEN_COMMA);
          state = 2408;
          stringNullableLiteralVarargs();
        }

        state = 2411;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_valuesContext traversalMethod_values() {
    dynamic _localctx = TraversalMethod_valuesContext(context, state);
    enterRule(_localctx, 320, RULE_traversalMethod_values);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2415;
      match(TOKEN_K_VALUES);
      state = 2416;
      match(TOKEN_LPAREN);
      state = 2417;
      stringNullableLiteralVarargs();
      state = 2418;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_whereContext traversalMethod_where() {
    dynamic _localctx = TraversalMethod_whereContext(context, state);
    enterRule(_localctx, 322, RULE_traversalMethod_where);
    try {
      state = 2437;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 98, context)) {
      case 1:
        _localctx = TraversalMethod_where_PContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2420;
        match(TOKEN_K_WHERE);
        state = 2421;
        match(TOKEN_LPAREN);
        state = 2422;
        traversalPredicate(0);
        state = 2423;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_where_String_PContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2425;
        match(TOKEN_K_WHERE);
        state = 2426;
        match(TOKEN_LPAREN);
        state = 2427;
        stringLiteral();
        state = 2428;
        match(TOKEN_COMMA);
        state = 2429;
        traversalPredicate(0);
        state = 2430;
        match(TOKEN_RPAREN);
        break;
      case 3:
        _localctx = TraversalMethod_where_TraversalContext(_localctx);
        enterOuterAlt(_localctx, 3);
        state = 2432;
        match(TOKEN_K_WHERE);
        state = 2433;
        match(TOKEN_LPAREN);
        state = 2434;
        nestedTraversal();
        state = 2435;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_withContext traversalMethod_with() {
    dynamic _localctx = TraversalMethod_withContext(context, state);
    enterRule(_localctx, 324, RULE_traversalMethod_with);
    try {
      state = 2461;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 102, context)) {
      case 1:
        _localctx = TraversalMethod_with_StringContext(_localctx);
        enterOuterAlt(_localctx, 1);
        state = 2439;
        match(TOKEN_K_WITH);
        state = 2440;
        match(TOKEN_LPAREN);
        state = 2443;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_CONNECTEDCOMPONENTU:
        case TOKEN_K_IOU:
        case TOKEN_K_PAGERANKU:
        case TOKEN_K_PEERPRESSUREU:
        case TOKEN_K_SHORTESTPATHU:
        case TOKEN_K_WITHOPTOPTIONS:
          state = 2441;
          withOptionKeys();
          break;
        case TOKEN_StringSuffixLiteral:
        case TOKEN_EmptyStringSuffixLiteral:
        case TOKEN_NonEmptyStringLiteral:
        case TOKEN_EmptyStringLiteral:
          state = 2442;
          stringLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2445;
        match(TOKEN_RPAREN);
        break;
      case 2:
        _localctx = TraversalMethod_with_String_ObjectContext(_localctx);
        enterOuterAlt(_localctx, 2);
        state = 2447;
        match(TOKEN_K_WITH);
        state = 2448;
        match(TOKEN_LPAREN);
        state = 2451;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_CONNECTEDCOMPONENTU:
        case TOKEN_K_IOU:
        case TOKEN_K_PAGERANKU:
        case TOKEN_K_PEERPRESSUREU:
        case TOKEN_K_SHORTESTPATHU:
        case TOKEN_K_WITHOPTOPTIONS:
          state = 2449;
          withOptionKeys();
          break;
        case TOKEN_StringSuffixLiteral:
        case TOKEN_EmptyStringSuffixLiteral:
        case TOKEN_NonEmptyStringLiteral:
        case TOKEN_EmptyStringLiteral:
          state = 2450;
          stringLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2453;
        match(TOKEN_COMMA);
        state = 2457;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_WITHOPTOPTIONS:
          state = 2454;
          withOptionsValues();
          break;
        case TOKEN_K_IOU:
          state = 2455;
          ioOptionsValues();
          break;
        case TOKEN_K_ADDE:
        case TOKEN_K_ADDV:
        case TOKEN_K_AGGREGATE:
        case TOKEN_K_ALL:
        case TOKEN_K_AND:
        case TOKEN_K_ANY:
        case TOKEN_K_AS:
        case TOKEN_K_ASBOOL:
        case TOKEN_K_ASDATE:
        case TOKEN_K_ASNUMBER:
        case TOKEN_K_ASSTRING:
        case TOKEN_K_BARRIER:
        case TOKEN_K_BIGDECIMAL:
        case TOKEN_K_BIGDECIMALU:
        case TOKEN_K_BIGINT:
        case TOKEN_K_BIGINTU:
        case TOKEN_K_BINARY:
        case TOKEN_K_BINARYC:
        case TOKEN_K_BINARYU:
        case TOKEN_K_BOOLEAN:
        case TOKEN_K_BOOLEANU:
        case TOKEN_K_BOTH:
        case TOKEN_K_BOTHU:
        case TOKEN_K_BOTHE:
        case TOKEN_K_BOTHV:
        case TOKEN_K_BRANCH:
        case TOKEN_K_BY:
        case TOKEN_K_BYTE:
        case TOKEN_K_BYTEU:
        case TOKEN_K_CALL:
        case TOKEN_K_CAP:
        case TOKEN_K_CARDINALITY:
        case TOKEN_K_CHAR:
        case TOKEN_K_CHARU:
        case TOKEN_K_CHOOSE:
        case TOKEN_K_COALESCE:
        case TOKEN_K_COIN:
        case TOKEN_K_COMBINE:
        case TOKEN_K_CONCAT:
        case TOKEN_K_CONJOIN:
        case TOKEN_K_CONNECTEDCOMPONENT:
        case TOKEN_K_CONSTANT:
        case TOKEN_K_COUNT:
        case TOKEN_K_CYCLICPATH:
        case TOKEN_K_DAY:
        case TOKEN_K_DATEADD:
        case TOKEN_K_DATEDIFF:
        case TOKEN_K_DATETIME:
        case TOKEN_K_DATETIMEC:
        case TOKEN_K_DATETIMEU:
        case TOKEN_K_DEDUP:
        case TOKEN_K_DIFFERENCE:
        case TOKEN_K_DISCARD:
        case TOKEN_K_DIRECTION:
        case TOKEN_K_DISJUNCT:
        case TOKEN_K_DOUBLE:
        case TOKEN_K_DOUBLEU:
        case TOKEN_K_DROP:
        case TOKEN_K_DT:
        case TOKEN_K_DURATION:
        case TOKEN_K_DURATIONC:
        case TOKEN_K_DURATIONU:
        case TOKEN_K_E:
        case TOKEN_K_EDGE:
        case TOKEN_K_EDGEU:
        case TOKEN_K_ELEMENTMAP:
        case TOKEN_K_ELEMENT:
        case TOKEN_K_EMIT:
        case TOKEN_K_FAIL:
        case TOKEN_K_FALSE:
        case TOKEN_K_FILTER:
        case TOKEN_K_FLATMAP:
        case TOKEN_K_FLOAT:
        case TOKEN_K_FLOATU:
        case TOKEN_K_FOLD:
        case TOKEN_K_FORMAT:
        case TOKEN_K_FROM:
        case TOKEN_K_GTYPE:
        case TOKEN_K_GROUPCOUNT:
        case TOKEN_K_GROUP:
        case TOKEN_K_GRAPH:
        case TOKEN_K_GRAPHU:
        case TOKEN_K_HAS:
        case TOKEN_K_HASID:
        case TOKEN_K_HASKEY:
        case TOKEN_K_HASLABEL:
        case TOKEN_K_HASNOT:
        case TOKEN_K_HASVALUE:
        case TOKEN_K_HOUR:
        case TOKEN_K_ID:
        case TOKEN_K_IDENTITY:
        case TOKEN_K_IN:
        case TOKEN_K_INU:
        case TOKEN_K_INE:
        case TOKEN_K_INDEX:
        case TOKEN_K_INFINITY:
        case TOKEN_K_INJECT:
        case TOKEN_K_INT:
        case TOKEN_K_INTU:
        case TOKEN_K_INTERSECT:
        case TOKEN_K_INV:
        case TOKEN_K_IS:
        case TOKEN_K_KEY:
        case TOKEN_K_LABEL:
        case TOKEN_K_LENGTH:
        case TOKEN_K_LIMIT:
        case TOKEN_K_LIST:
        case TOKEN_K_LISTU:
        case TOKEN_K_LOCAL:
        case TOKEN_K_LONG:
        case TOKEN_K_LONGU:
        case TOKEN_K_LOOPS:
        case TOKEN_K_LTRIM:
        case TOKEN_K_MAP:
        case TOKEN_K_MAPU:
        case TOKEN_K_MATCH:
        case TOKEN_K_MATH:
        case TOKEN_K_MAX:
        case TOKEN_K_MEAN:
        case TOKEN_K_MERGEU:
        case TOKEN_K_MERGE:
        case TOKEN_K_MERGEE:
        case TOKEN_K_MERGEV:
        case TOKEN_K_MIN:
        case TOKEN_K_MINUTE:
        case TOKEN_K_NAN:
        case TOKEN_K_NONE:
        case TOKEN_K_NOT:
        case TOKEN_K_NULL:
        case TOKEN_K_NULLU:
        case TOKEN_K_NUMBER:
        case TOKEN_K_NUMBERU:
        case TOKEN_K_ONCREATE:
        case TOKEN_K_ONMATCH:
        case TOKEN_K_OPTION:
        case TOKEN_K_OPTIONAL:
        case TOKEN_K_ORDER:
        case TOKEN_K_OR:
        case TOKEN_K_OTHERV:
        case TOKEN_K_OUTU:
        case TOKEN_K_OUT:
        case TOKEN_K_OUTE:
        case TOKEN_K_OUTV:
        case TOKEN_K_PAGERANK:
        case TOKEN_K_PATH:
        case TOKEN_K_PATHU:
        case TOKEN_K_PEERPRESSURE:
        case TOKEN_K_PICK:
        case TOKEN_K_PROFILE:
        case TOKEN_K_PROJECT:
        case TOKEN_K_PROPERTIES:
        case TOKEN_K_PROPERTYMAP:
        case TOKEN_K_PROPERTY:
        case TOKEN_K_PROPERTYU:
        case TOKEN_K_PRODUCT:
        case TOKEN_K_RANGE:
        case TOKEN_K_READ:
        case TOKEN_K_REPLACE:
        case TOKEN_K_REPEAT:
        case TOKEN_K_REVERSE:
        case TOKEN_K_RTRIM:
        case TOKEN_K_SACK:
        case TOKEN_K_SAMPLE:
        case TOKEN_K_SECOND:
        case TOKEN_K_SELECT:
        case TOKEN_K_SET:
        case TOKEN_K_SETU:
        case TOKEN_K_SHORTESTPATH:
        case TOKEN_K_SHORT:
        case TOKEN_K_SHORTU:
        case TOKEN_K_SIDEEFFECT:
        case TOKEN_K_SIMPLEPATH:
        case TOKEN_K_SINGLE:
        case TOKEN_K_SKIP:
        case TOKEN_K_SPLIT:
        case TOKEN_K_STRING:
        case TOKEN_K_STRINGU:
        case TOKEN_K_SUBGRAPH:
        case TOKEN_K_SUBSTRING:
        case TOKEN_K_SUM:
        case TOKEN_K_T:
        case TOKEN_K_TAIL:
        case TOKEN_K_TIMELIMIT:
        case TOKEN_K_TIMES:
        case TOKEN_K_TO:
        case TOKEN_K_TOLOWER:
        case TOKEN_K_TOUPPER:
        case TOKEN_K_TOE:
        case TOKEN_K_TOV:
        case TOKEN_K_TREE:
        case TOKEN_K_TREEU:
        case TOKEN_K_TRIM:
        case TOKEN_K_TRUE:
        case TOKEN_K_UNFOLD:
        case TOKEN_K_UNION:
        case TOKEN_K_UNPRODUCTIVE:
        case TOKEN_K_UNTIL:
        case TOKEN_K_UUID:
        case TOKEN_K_UUIDL:
        case TOKEN_K_V:
        case TOKEN_K_VALUEMAP:
        case TOKEN_K_VALUES:
        case TOKEN_K_VALUE:
        case TOKEN_K_VERTEX:
        case TOKEN_K_VERTEXU:
        case TOKEN_K_VPROPERTY:
        case TOKEN_K_VPROPERTYU:
        case TOKEN_K_WHERE:
        case TOKEN_K_WITH:
        case TOKEN_K_WRITE:
        case TOKEN_IntegerLiteral:
        case TOKEN_FloatingPointLiteral:
        case TOKEN_SignedInfLiteral:
        case TOKEN_CharacterLiteral:
        case TOKEN_StringSuffixLiteral:
        case TOKEN_EmptyStringSuffixLiteral:
        case TOKEN_NonEmptyStringLiteral:
        case TOKEN_EmptyStringLiteral:
        case TOKEN_LBRACE:
        case TOKEN_LBRACK:
        case TOKEN_TRAVERSAL_ROOT:
        case TOKEN_ANON_TRAVERSAL_ROOT:
          state = 2456;
          genericLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2459;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMethod_writeContext traversalMethod_write() {
    dynamic _localctx = TraversalMethod_writeContext(context, state);
    enterRule(_localctx, 326, RULE_traversalMethod_write);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2463;
      match(TOKEN_K_WRITE);
      state = 2464;
      match(TOKEN_LPAREN);
      state = 2465;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalStrategyContext traversalStrategy() {
    dynamic _localctx = TraversalStrategyContext(context, state);
    enterRule(_localctx, 328, RULE_traversalStrategy);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 2468;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_K_NEW) {
        state = 2467;
        match(TOKEN_K_NEW);
      }

      state = 2470;
      classType();
      state = 2483;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_LPAREN) {
        state = 2471;
        match(TOKEN_LPAREN);
        state = 2480;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -2) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1 << (_la - 64)) & -1) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1 << (_la - 128)) & -1) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1 << (_la - 192)) & -1) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1 << (_la - 256)) & -9223336852482686977) != 0) || _la == TOKEN_Identifier) {
          state = 2472;
          configuration();
          state = 2477;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
          while (_la == TOKEN_COMMA) {
            state = 2473;
            match(TOKEN_COMMA);
            state = 2474;
            configuration();
            state = 2479;
            errorHandler.sync(this);
            _la = tokenStream.LA(1)!;
          }
        }

        state = 2482;
        match(TOKEN_RPAREN);
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConfigurationContext configuration() {
    dynamic _localctx = ConfigurationContext(context, state);
    enterRule(_localctx, 330, RULE_configuration);
    try {
      enterOuterAlt(_localctx, 1);
      state = 2487;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ADDALL:
      case TOKEN_K_ADDE:
      case TOKEN_K_ADDV:
      case TOKEN_K_AGGREGATE:
      case TOKEN_K_ALL:
      case TOKEN_K_AND:
      case TOKEN_K_ANY:
      case TOKEN_K_AS:
      case TOKEN_K_ASBOOL:
      case TOKEN_K_ASC:
      case TOKEN_K_ASDATE:
      case TOKEN_K_ASNUMBER:
      case TOKEN_K_ASSTRING:
      case TOKEN_K_ASSIGN:
      case TOKEN_K_BARRIER:
      case TOKEN_K_BARRIERU:
      case TOKEN_K_BEGIN:
      case TOKEN_K_BETWEEN:
      case TOKEN_K_BIGDECIMAL:
      case TOKEN_K_BIGDECIMALU:
      case TOKEN_K_BIGINT:
      case TOKEN_K_BIGINTU:
      case TOKEN_K_BINARY:
      case TOKEN_K_BINARYC:
      case TOKEN_K_BINARYU:
      case TOKEN_K_BOOLEAN:
      case TOKEN_K_BOOLEANU:
      case TOKEN_K_BOTH:
      case TOKEN_K_BOTHU:
      case TOKEN_K_BOTHE:
      case TOKEN_K_BOTHV:
      case TOKEN_K_BRANCH:
      case TOKEN_K_BY:
      case TOKEN_K_BYTE:
      case TOKEN_K_BYTEU:
      case TOKEN_K_CALL:
      case TOKEN_K_CAP:
      case TOKEN_K_CARDINALITY:
      case TOKEN_K_CHAR:
      case TOKEN_K_CHARU:
      case TOKEN_K_CHOOSE:
      case TOKEN_K_COALESCE:
      case TOKEN_K_COIN:
      case TOKEN_K_COLUMN:
      case TOKEN_K_COMBINE:
      case TOKEN_K_COMMIT:
      case TOKEN_K_COMPONENT:
      case TOKEN_K_CONCAT:
      case TOKEN_K_CONJOIN:
      case TOKEN_K_CONNECTEDCOMPONENT:
      case TOKEN_K_CONNECTEDCOMPONENTU:
      case TOKEN_K_CONSTANT:
      case TOKEN_K_CONTAINING:
      case TOKEN_K_COUNT:
      case TOKEN_K_CYCLICPATH:
      case TOKEN_K_DAY:
      case TOKEN_K_DATEADD:
      case TOKEN_K_DATEDIFF:
      case TOKEN_K_DATETIME:
      case TOKEN_K_DATETIMEC:
      case TOKEN_K_DATETIMEU:
      case TOKEN_K_DECR:
      case TOKEN_K_DEDUP:
      case TOKEN_K_DESC:
      case TOKEN_K_DIFFERENCE:
      case TOKEN_K_DISCARD:
      case TOKEN_K_DIRECTION:
      case TOKEN_K_DISJUNCT:
      case TOKEN_K_DISTANCE:
      case TOKEN_K_DIV:
      case TOKEN_K_DOUBLE:
      case TOKEN_K_DOUBLEU:
      case TOKEN_K_DROP:
      case TOKEN_K_DT:
      case TOKEN_K_DURATION:
      case TOKEN_K_DURATIONC:
      case TOKEN_K_DURATIONU:
      case TOKEN_K_E:
      case TOKEN_K_EDGE:
      case TOKEN_K_EDGEU:
      case TOKEN_K_EDGES:
      case TOKEN_K_ELEMENTMAP:
      case TOKEN_K_ELEMENT:
      case TOKEN_K_EMIT:
      case TOKEN_K_ENDINGWITH:
      case TOKEN_K_EQ:
      case TOKEN_K_EXPLAIN:
      case TOKEN_K_FAIL:
      case TOKEN_K_FALSE:
      case TOKEN_K_FILTER:
      case TOKEN_K_FIRST:
      case TOKEN_K_FLATMAP:
      case TOKEN_K_FLOAT:
      case TOKEN_K_FLOATU:
      case TOKEN_K_FOLD:
      case TOKEN_K_FORMAT:
      case TOKEN_K_FROM:
      case TOKEN_K_GLOBAL:
      case TOKEN_K_GT:
      case TOKEN_K_GTE:
      case TOKEN_K_GTYPE:
      case TOKEN_K_GRAPHML:
      case TOKEN_K_GRAPHSON:
      case TOKEN_K_GROUPCOUNT:
      case TOKEN_K_GROUP:
      case TOKEN_K_GRYO:
      case TOKEN_K_GRAPH:
      case TOKEN_K_GRAPHU:
      case TOKEN_K_HAS:
      case TOKEN_K_HASID:
      case TOKEN_K_HASKEY:
      case TOKEN_K_HASLABEL:
      case TOKEN_K_HASNEXT:
      case TOKEN_K_HASNOT:
      case TOKEN_K_HASVALUE:
      case TOKEN_K_HOUR:
      case TOKEN_K_ID:
      case TOKEN_K_IDENTITY:
      case TOKEN_K_IDS:
      case TOKEN_K_IN:
      case TOKEN_K_INU:
      case TOKEN_K_INE:
      case TOKEN_K_INCLUDEEDGES:
      case TOKEN_K_INCR:
      case TOKEN_K_INDEXER:
      case TOKEN_K_INDEX:
      case TOKEN_K_INFINITY:
      case TOKEN_K_INJECT:
      case TOKEN_K_INSIDE:
      case TOKEN_K_INT:
      case TOKEN_K_INTU:
      case TOKEN_K_INTERSECT:
      case TOKEN_K_INV:
      case TOKEN_K_IOU:
      case TOKEN_K_IO:
      case TOKEN_K_IS:
      case TOKEN_K_ITERATE:
      case TOKEN_K_KEY:
      case TOKEN_K_KEYS:
      case TOKEN_K_LABELS:
      case TOKEN_K_LABEL:
      case TOKEN_K_LAST:
      case TOKEN_K_LENGTH:
      case TOKEN_K_LIMIT:
      case TOKEN_K_LIST:
      case TOKEN_K_LISTU:
      case TOKEN_K_LOCAL:
      case TOKEN_K_LONG:
      case TOKEN_K_LONGU:
      case TOKEN_K_LOOPS:
      case TOKEN_K_LT:
      case TOKEN_K_LTE:
      case TOKEN_K_LTRIM:
      case TOKEN_K_MAP:
      case TOKEN_K_MAPU:
      case TOKEN_K_MATCH:
      case TOKEN_K_MATH:
      case TOKEN_K_MAX:
      case TOKEN_K_MAXDISTANCE:
      case TOKEN_K_MEAN:
      case TOKEN_K_MERGEU:
      case TOKEN_K_MERGE:
      case TOKEN_K_MERGEE:
      case TOKEN_K_MERGEV:
      case TOKEN_K_MIN:
      case TOKEN_K_MINUTE:
      case TOKEN_K_MINUS:
      case TOKEN_K_MIXED:
      case TOKEN_K_MULT:
      case TOKEN_K_N:
      case TOKEN_K_NAN:
      case TOKEN_K_NEGATE:
      case TOKEN_K_NEXT:
      case TOKEN_K_NONE:
      case TOKEN_K_NOTREGEX:
      case TOKEN_K_NOTCONTAINING:
      case TOKEN_K_NOTENDINGWITH:
      case TOKEN_K_NOTSTARTINGWITH:
      case TOKEN_K_NOT:
      case TOKEN_K_NEQ:
      case TOKEN_K_NEW:
      case TOKEN_K_NORMSACK:
      case TOKEN_K_NULL:
      case TOKEN_K_NULLU:
      case TOKEN_K_NUMBER:
      case TOKEN_K_NUMBERU:
      case TOKEN_K_ONCREATE:
      case TOKEN_K_ONMATCH:
      case TOKEN_K_OPERATOR:
      case TOKEN_K_OPTION:
      case TOKEN_K_OPTIONAL:
      case TOKEN_K_ORDERU:
      case TOKEN_K_ORDER:
      case TOKEN_K_OR:
      case TOKEN_K_OTHERV:
      case TOKEN_K_OUTU:
      case TOKEN_K_OUT:
      case TOKEN_K_OUTE:
      case TOKEN_K_OUTSIDE:
      case TOKEN_K_OUTV:
      case TOKEN_K_P:
      case TOKEN_K_PAGERANKU:
      case TOKEN_K_PAGERANK:
      case TOKEN_K_PATH:
      case TOKEN_K_PATHU:
      case TOKEN_K_PEERPRESSUREU:
      case TOKEN_K_PEERPRESSURE:
      case TOKEN_K_PICK:
      case TOKEN_K_POP:
      case TOKEN_K_PROFILE:
      case TOKEN_K_PROJECT:
      case TOKEN_K_PROPERTIES:
      case TOKEN_K_PROPERTYMAP:
      case TOKEN_K_PROPERTYNAME:
      case TOKEN_K_PROPERTY:
      case TOKEN_K_PROPERTYU:
      case TOKEN_K_PRODUCT:
      case TOKEN_K_RANGE:
      case TOKEN_K_READ:
      case TOKEN_K_READER:
      case TOKEN_K_REGEX:
      case TOKEN_K_REPLACE:
      case TOKEN_K_REPEAT:
      case TOKEN_K_REVERSE:
      case TOKEN_K_ROLLBACK:
      case TOKEN_K_RTRIM:
      case TOKEN_K_SACK:
      case TOKEN_K_SAMPLE:
      case TOKEN_K_SCOPE:
      case TOKEN_K_SECOND:
      case TOKEN_K_SELECT:
      case TOKEN_K_SET:
      case TOKEN_K_SETU:
      case TOKEN_K_SHORTESTPATHU:
      case TOKEN_K_SHORTESTPATH:
      case TOKEN_K_SHUFFLE:
      case TOKEN_K_SHORT:
      case TOKEN_K_SHORTU:
      case TOKEN_K_SIDEEFFECT:
      case TOKEN_K_SIMPLEPATH:
      case TOKEN_K_SINGLE:
      case TOKEN_K_SKIP:
      case TOKEN_K_SPLIT:
      case TOKEN_K_STARTINGWITH:
      case TOKEN_K_STRING:
      case TOKEN_K_STRINGU:
      case TOKEN_K_SUBGRAPH:
      case TOKEN_K_SUBSTRING:
      case TOKEN_K_SUM:
      case TOKEN_K_SUMLONG:
      case TOKEN_K_T:
      case TOKEN_K_TAIL:
      case TOKEN_K_TARGET:
      case TOKEN_K_TEXTP:
      case TOKEN_K_TIMELIMIT:
      case TOKEN_K_TIMES:
      case TOKEN_K_TO:
      case TOKEN_K_TOBULKSET:
      case TOKEN_K_TOKENS:
      case TOKEN_K_TOLIST:
      case TOKEN_K_TOLOWER:
      case TOKEN_K_TOSET:
      case TOKEN_K_TOSTRING:
      case TOKEN_K_TOUPPER:
      case TOKEN_K_TOE:
      case TOKEN_K_TOV:
      case TOKEN_K_TREE:
      case TOKEN_K_TREEU:
      case TOKEN_K_TRIM:
      case TOKEN_K_TRUE:
      case TOKEN_K_TRYNEXT:
      case TOKEN_K_TYPEOF:
      case TOKEN_K_TX:
      case TOKEN_K_UNFOLD:
      case TOKEN_K_UNION:
      case TOKEN_K_UNPRODUCTIVE:
      case TOKEN_K_UNTIL:
      case TOKEN_K_UUID:
      case TOKEN_K_UUIDL:
      case TOKEN_K_V:
      case TOKEN_K_VALUEMAP:
      case TOKEN_K_VALUES:
      case TOKEN_K_VALUE:
      case TOKEN_K_VERTEX:
      case TOKEN_K_VERTEXU:
      case TOKEN_K_VPROPERTY:
      case TOKEN_K_VPROPERTYU:
      case TOKEN_K_WHERE:
      case TOKEN_K_WITH:
      case TOKEN_K_WITHBULK:
      case TOKEN_K_WITHIN:
      case TOKEN_K_WITHOPTOPTIONS:
      case TOKEN_K_WITHOUT:
      case TOKEN_K_WITHOUTSTRATEGIES:
      case TOKEN_K_WITHPATH:
      case TOKEN_K_WITHSACK:
      case TOKEN_K_WITHSIDEEFFECT:
      case TOKEN_K_WITHSTRATEGIES:
      case TOKEN_K_WRITE:
      case TOKEN_K_WRITER:
      case TOKEN_TRAVERSAL_ROOT:
        state = 2485;
        keyword();
        break;
      case TOKEN_Identifier:
        state = 2486;
        nakedKey();
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 2489;
      match(TOKEN_COLON);
      state = 2490;
      genericArgument();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalScopeContext traversalScope() {
    dynamic _localctx = TraversalScopeContext(context, state);
    enterRule(_localctx, 332, RULE_traversalScope);
    try {
      state = 2500;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 108, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2492;
        match(TOKEN_K_LOCAL);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2493;
        match(TOKEN_K_SCOPE);
        state = 2494;
        match(TOKEN_DOT);
        state = 2495;
        match(TOKEN_K_LOCAL);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2496;
        match(TOKEN_K_GLOBAL);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2497;
        match(TOKEN_K_SCOPE);
        state = 2498;
        match(TOKEN_DOT);
        state = 2499;
        match(TOKEN_K_GLOBAL);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalBarrierContext traversalBarrier() {
    dynamic _localctx = TraversalBarrierContext(context, state);
    enterRule(_localctx, 334, RULE_traversalBarrier);
    try {
      state = 2506;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_NORMSACK:
        enterOuterAlt(_localctx, 1);
        state = 2502;
        match(TOKEN_K_NORMSACK);
        break;
      case TOKEN_K_BARRIERU:
        enterOuterAlt(_localctx, 2);
        state = 2503;
        match(TOKEN_K_BARRIERU);
        state = 2504;
        match(TOKEN_DOT);
        state = 2505;
        match(TOKEN_K_NORMSACK);
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTContext traversalT() {
    dynamic _localctx = TraversalTContext(context, state);
    enterRule(_localctx, 336, RULE_traversalT);
    try {
      state = 2510;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ID:
      case TOKEN_K_KEY:
      case TOKEN_K_LABEL:
      case TOKEN_K_VALUE:
        enterOuterAlt(_localctx, 1);
        state = 2508;
        traversalTShort();
        break;
      case TOKEN_K_T:
        enterOuterAlt(_localctx, 2);
        state = 2509;
        traversalTLong();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTShortContext traversalTShort() {
    dynamic _localctx = TraversalTShortContext(context, state);
    enterRule(_localctx, 338, RULE_traversalTShort);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 2512;
      _la = tokenStream.LA(1)!;
      if (!(((((_la - 117)) & ~0x3f) == 0 && ((1 << (_la - 117)) & 18874369) != 0) || _la == TOKEN_K_VALUE)) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTLongContext traversalTLong() {
    dynamic _localctx = TraversalTLongContext(context, state);
    enterRule(_localctx, 340, RULE_traversalTLong);
    try {
      state = 2526;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 111, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2514;
        match(TOKEN_K_T);
        state = 2515;
        match(TOKEN_DOT);
        state = 2516;
        match(TOKEN_K_ID);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2517;
        match(TOKEN_K_T);
        state = 2518;
        match(TOKEN_DOT);
        state = 2519;
        match(TOKEN_K_LABEL);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2520;
        match(TOKEN_K_T);
        state = 2521;
        match(TOKEN_DOT);
        state = 2522;
        match(TOKEN_K_KEY);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2523;
        match(TOKEN_K_T);
        state = 2524;
        match(TOKEN_DOT);
        state = 2525;
        match(TOKEN_K_VALUE);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalMergeContext traversalMerge() {
    dynamic _localctx = TraversalMergeContext(context, state);
    enterRule(_localctx, 342, RULE_traversalMerge);
    try {
      state = 2544;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 112, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2528;
        match(TOKEN_K_ONCREATE);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2529;
        match(TOKEN_K_MERGEU);
        state = 2530;
        match(TOKEN_DOT);
        state = 2531;
        match(TOKEN_K_ONCREATE);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2532;
        match(TOKEN_K_ONMATCH);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2533;
        match(TOKEN_K_MERGEU);
        state = 2534;
        match(TOKEN_DOT);
        state = 2535;
        match(TOKEN_K_ONMATCH);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2536;
        match(TOKEN_K_OUTV);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2537;
        match(TOKEN_K_MERGEU);
        state = 2538;
        match(TOKEN_DOT);
        state = 2539;
        match(TOKEN_K_OUTV);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2540;
        match(TOKEN_K_INV);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2541;
        match(TOKEN_K_MERGEU);
        state = 2542;
        match(TOKEN_DOT);
        state = 2543;
        match(TOKEN_K_INV);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalOrderContext traversalOrder() {
    dynamic _localctx = TraversalOrderContext(context, state);
    enterRule(_localctx, 344, RULE_traversalOrder);
    try {
      state = 2558;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 113, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2546;
        match(TOKEN_K_ASC);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2547;
        match(TOKEN_K_ORDERU);
        state = 2548;
        match(TOKEN_DOT);
        state = 2549;
        match(TOKEN_K_ASC);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2550;
        match(TOKEN_K_DESC);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2551;
        match(TOKEN_K_ORDERU);
        state = 2552;
        match(TOKEN_DOT);
        state = 2553;
        match(TOKEN_K_DESC);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2554;
        match(TOKEN_K_SHUFFLE);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2555;
        match(TOKEN_K_ORDERU);
        state = 2556;
        match(TOKEN_DOT);
        state = 2557;
        match(TOKEN_K_SHUFFLE);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalDirectionContext traversalDirection() {
    dynamic _localctx = TraversalDirectionContext(context, state);
    enterRule(_localctx, 346, RULE_traversalDirection);
    try {
      state = 2562;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_BOTHU:
      case TOKEN_K_FROM:
      case TOKEN_K_INU:
      case TOKEN_K_OUTU:
      case TOKEN_K_TO:
        enterOuterAlt(_localctx, 1);
        state = 2560;
        traversalDirectionShort();
        break;
      case TOKEN_K_DIRECTION:
        enterOuterAlt(_localctx, 2);
        state = 2561;
        traversalDirectionLong();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalDirectionShortContext traversalDirectionShort() {
    dynamic _localctx = TraversalDirectionShortContext(context, state);
    enterRule(_localctx, 348, RULE_traversalDirectionShort);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 2564;
      _la = tokenStream.LA(1)!;
      if (!(_la == TOKEN_K_BOTHU || _la == TOKEN_K_FROM || _la == TOKEN_K_INU || _la == TOKEN_K_OUTU || _la == TOKEN_K_TO)) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalDirectionLongContext traversalDirectionLong() {
    dynamic _localctx = TraversalDirectionLongContext(context, state);
    enterRule(_localctx, 350, RULE_traversalDirectionLong);
    try {
      state = 2581;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 115, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2566;
        match(TOKEN_K_DIRECTION);
        state = 2567;
        match(TOKEN_DOT);
        state = 2568;
        match(TOKEN_K_INU);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2569;
        match(TOKEN_K_DIRECTION);
        state = 2570;
        match(TOKEN_DOT);
        state = 2571;
        match(TOKEN_K_FROM);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2572;
        match(TOKEN_K_DIRECTION);
        state = 2573;
        match(TOKEN_DOT);
        state = 2574;
        match(TOKEN_K_OUTU);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2575;
        match(TOKEN_K_DIRECTION);
        state = 2576;
        match(TOKEN_DOT);
        state = 2577;
        match(TOKEN_K_TO);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2578;
        match(TOKEN_K_DIRECTION);
        state = 2579;
        match(TOKEN_DOT);
        state = 2580;
        match(TOKEN_K_BOTHU);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalCardinalityContext traversalCardinality() {
    dynamic _localctx = TraversalCardinalityContext(context, state);
    enterRule(_localctx, 352, RULE_traversalCardinality);
    try {
      state = 2625;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 119, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2587;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_CARDINALITY:
          state = 2583;
          match(TOKEN_K_CARDINALITY);
          state = 2584;
          match(TOKEN_DOT);
          state = 2585;
          match(TOKEN_K_SINGLE);
          break;
        case TOKEN_K_SINGLE:
          state = 2586;
          match(TOKEN_K_SINGLE);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2589;
        match(TOKEN_LPAREN);
        state = 2590;
        genericLiteral();
        state = 2591;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2597;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_CARDINALITY:
          state = 2593;
          match(TOKEN_K_CARDINALITY);
          state = 2594;
          match(TOKEN_DOT);
          state = 2595;
          match(TOKEN_K_SET);
          break;
        case TOKEN_K_SET:
          state = 2596;
          match(TOKEN_K_SET);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2599;
        match(TOKEN_LPAREN);
        state = 2600;
        genericLiteral();
        state = 2601;
        match(TOKEN_RPAREN);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2607;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_CARDINALITY:
          state = 2603;
          match(TOKEN_K_CARDINALITY);
          state = 2604;
          match(TOKEN_DOT);
          state = 2605;
          match(TOKEN_K_LIST);
          break;
        case TOKEN_K_LIST:
          state = 2606;
          match(TOKEN_K_LIST);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 2609;
        match(TOKEN_LPAREN);
        state = 2610;
        genericLiteral();
        state = 2611;
        match(TOKEN_RPAREN);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2613;
        match(TOKEN_K_SINGLE);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2614;
        match(TOKEN_K_CARDINALITY);
        state = 2615;
        match(TOKEN_DOT);
        state = 2616;
        match(TOKEN_K_SINGLE);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2617;
        match(TOKEN_K_SET);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2618;
        match(TOKEN_K_CARDINALITY);
        state = 2619;
        match(TOKEN_DOT);
        state = 2620;
        match(TOKEN_K_SET);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2621;
        match(TOKEN_K_LIST);
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        state = 2622;
        match(TOKEN_K_CARDINALITY);
        state = 2623;
        match(TOKEN_DOT);
        state = 2624;
        match(TOKEN_K_LIST);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalColumnContext traversalColumn() {
    dynamic _localctx = TraversalColumnContext(context, state);
    enterRule(_localctx, 354, RULE_traversalColumn);
    try {
      state = 2635;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 120, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2627;
        match(TOKEN_K_KEYS);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2628;
        match(TOKEN_K_COLUMN);
        state = 2629;
        match(TOKEN_DOT);
        state = 2630;
        match(TOKEN_K_KEYS);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2631;
        match(TOKEN_K_VALUES);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2632;
        match(TOKEN_K_COLUMN);
        state = 2633;
        match(TOKEN_DOT);
        state = 2634;
        match(TOKEN_K_VALUES);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPopContext traversalPop() {
    dynamic _localctx = TraversalPopContext(context, state);
    enterRule(_localctx, 356, RULE_traversalPop);
    try {
      state = 2653;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 121, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2637;
        match(TOKEN_K_FIRST);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2638;
        match(TOKEN_K_POP);
        state = 2639;
        match(TOKEN_DOT);
        state = 2640;
        match(TOKEN_K_FIRST);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2641;
        match(TOKEN_K_LAST);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2642;
        match(TOKEN_K_POP);
        state = 2643;
        match(TOKEN_DOT);
        state = 2644;
        match(TOKEN_K_LAST);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2645;
        match(TOKEN_K_ALL);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2646;
        match(TOKEN_K_POP);
        state = 2647;
        match(TOKEN_DOT);
        state = 2648;
        match(TOKEN_K_ALL);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2649;
        match(TOKEN_K_MIXED);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2650;
        match(TOKEN_K_POP);
        state = 2651;
        match(TOKEN_DOT);
        state = 2652;
        match(TOKEN_K_MIXED);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalOperatorContext traversalOperator() {
    dynamic _localctx = TraversalOperatorContext(context, state);
    enterRule(_localctx, 358, RULE_traversalOperator);
    try {
      state = 2699;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 122, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2655;
        match(TOKEN_K_ADDALL);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2656;
        match(TOKEN_K_OPERATOR);
        state = 2657;
        match(TOKEN_DOT);
        state = 2658;
        match(TOKEN_K_ADDALL);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2659;
        match(TOKEN_K_AND);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2660;
        match(TOKEN_K_OPERATOR);
        state = 2661;
        match(TOKEN_DOT);
        state = 2662;
        match(TOKEN_K_AND);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2663;
        match(TOKEN_K_ASSIGN);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2664;
        match(TOKEN_K_OPERATOR);
        state = 2665;
        match(TOKEN_DOT);
        state = 2666;
        match(TOKEN_K_ASSIGN);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2667;
        match(TOKEN_K_DIV);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2668;
        match(TOKEN_K_OPERATOR);
        state = 2669;
        match(TOKEN_DOT);
        state = 2670;
        match(TOKEN_K_DIV);
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        state = 2671;
        match(TOKEN_K_MAX);
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        state = 2672;
        match(TOKEN_K_OPERATOR);
        state = 2673;
        match(TOKEN_DOT);
        state = 2674;
        match(TOKEN_K_MAX);
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        state = 2675;
        match(TOKEN_K_MIN);
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        state = 2676;
        match(TOKEN_K_OPERATOR);
        state = 2677;
        match(TOKEN_DOT);
        state = 2678;
        match(TOKEN_K_MIN);
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        state = 2679;
        match(TOKEN_K_MINUS);
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        state = 2680;
        match(TOKEN_K_OPERATOR);
        state = 2681;
        match(TOKEN_DOT);
        state = 2682;
        match(TOKEN_K_MINUS);
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        state = 2683;
        match(TOKEN_K_MULT);
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        state = 2684;
        match(TOKEN_K_OPERATOR);
        state = 2685;
        match(TOKEN_DOT);
        state = 2686;
        match(TOKEN_K_MULT);
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        state = 2687;
        match(TOKEN_K_OR);
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        state = 2688;
        match(TOKEN_K_OPERATOR);
        state = 2689;
        match(TOKEN_DOT);
        state = 2690;
        match(TOKEN_K_OR);
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        state = 2691;
        match(TOKEN_K_SUM);
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        state = 2692;
        match(TOKEN_K_OPERATOR);
        state = 2693;
        match(TOKEN_DOT);
        state = 2694;
        match(TOKEN_K_SUM);
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        state = 2695;
        match(TOKEN_K_SUMLONG);
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        state = 2696;
        match(TOKEN_K_OPERATOR);
        state = 2697;
        match(TOKEN_DOT);
        state = 2698;
        match(TOKEN_K_SUMLONG);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPickContext traversalPick() {
    dynamic _localctx = TraversalPickContext(context, state);
    enterRule(_localctx, 360, RULE_traversalPick);
    try {
      state = 2713;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 123, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2701;
        match(TOKEN_K_ANY);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2702;
        match(TOKEN_K_PICK);
        state = 2703;
        match(TOKEN_DOT);
        state = 2704;
        match(TOKEN_K_ANY);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2705;
        match(TOKEN_K_NONE);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2706;
        match(TOKEN_K_PICK);
        state = 2707;
        match(TOKEN_DOT);
        state = 2708;
        match(TOKEN_K_NONE);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2709;
        match(TOKEN_K_UNPRODUCTIVE);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2710;
        match(TOKEN_K_PICK);
        state = 2711;
        match(TOKEN_DOT);
        state = 2712;
        match(TOKEN_K_UNPRODUCTIVE);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalDTContext traversalDT() {
    dynamic _localctx = TraversalDTContext(context, state);
    enterRule(_localctx, 362, RULE_traversalDT);
    try {
      state = 2731;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 124, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2715;
        match(TOKEN_K_SECOND);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2716;
        match(TOKEN_K_DT);
        state = 2717;
        match(TOKEN_DOT);
        state = 2718;
        match(TOKEN_K_SECOND);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2719;
        match(TOKEN_K_MINUTE);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2720;
        match(TOKEN_K_DT);
        state = 2721;
        match(TOKEN_DOT);
        state = 2722;
        match(TOKEN_K_MINUTE);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2723;
        match(TOKEN_K_HOUR);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2724;
        match(TOKEN_K_DT);
        state = 2725;
        match(TOKEN_DOT);
        state = 2726;
        match(TOKEN_K_HOUR);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2727;
        match(TOKEN_K_DAY);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2728;
        match(TOKEN_K_DT);
        state = 2729;
        match(TOKEN_DOT);
        state = 2730;
        match(TOKEN_K_DAY);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalGTypeContext traversalGType() {
    dynamic _localctx = TraversalGTypeContext(context, state);
    enterRule(_localctx, 364, RULE_traversalGType);
    try {
      state = 2949;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 125, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 2733;
        match(TOKEN_K_BIGDECIMAL);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 2734;
        match(TOKEN_K_GTYPE);
        state = 2735;
        match(TOKEN_DOT);
        state = 2736;
        match(TOKEN_K_BIGDECIMAL);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 2737;
        match(TOKEN_K_BIGDECIMALU);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 2738;
        match(TOKEN_K_GTYPE);
        state = 2739;
        match(TOKEN_DOT);
        state = 2740;
        match(TOKEN_K_BIGDECIMALU);
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 2741;
        match(TOKEN_K_BIGINT);
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 2742;
        match(TOKEN_K_GTYPE);
        state = 2743;
        match(TOKEN_DOT);
        state = 2744;
        match(TOKEN_K_BIGINT);
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 2745;
        match(TOKEN_K_BIGINTU);
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 2746;
        match(TOKEN_K_GTYPE);
        state = 2747;
        match(TOKEN_DOT);
        state = 2748;
        match(TOKEN_K_BIGINTU);
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        state = 2749;
        match(TOKEN_K_BINARY);
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        state = 2750;
        match(TOKEN_K_GTYPE);
        state = 2751;
        match(TOKEN_DOT);
        state = 2752;
        match(TOKEN_K_BINARY);
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        state = 2753;
        match(TOKEN_K_BINARYU);
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        state = 2754;
        match(TOKEN_K_GTYPE);
        state = 2755;
        match(TOKEN_DOT);
        state = 2756;
        match(TOKEN_K_BINARYU);
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        state = 2757;
        match(TOKEN_K_BOOLEAN);
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        state = 2758;
        match(TOKEN_K_GTYPE);
        state = 2759;
        match(TOKEN_DOT);
        state = 2760;
        match(TOKEN_K_BOOLEAN);
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        state = 2761;
        match(TOKEN_K_BOOLEANU);
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        state = 2762;
        match(TOKEN_K_GTYPE);
        state = 2763;
        match(TOKEN_DOT);
        state = 2764;
        match(TOKEN_K_BOOLEANU);
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        state = 2765;
        match(TOKEN_K_BYTE);
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        state = 2766;
        match(TOKEN_K_GTYPE);
        state = 2767;
        match(TOKEN_DOT);
        state = 2768;
        match(TOKEN_K_BYTE);
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        state = 2769;
        match(TOKEN_K_BYTEU);
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        state = 2770;
        match(TOKEN_K_GTYPE);
        state = 2771;
        match(TOKEN_DOT);
        state = 2772;
        match(TOKEN_K_BYTEU);
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        state = 2773;
        match(TOKEN_K_CHAR);
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        state = 2774;
        match(TOKEN_K_GTYPE);
        state = 2775;
        match(TOKEN_DOT);
        state = 2776;
        match(TOKEN_K_CHAR);
        break;
      case 23:
        enterOuterAlt(_localctx, 23);
        state = 2777;
        match(TOKEN_K_CHARU);
        break;
      case 24:
        enterOuterAlt(_localctx, 24);
        state = 2778;
        match(TOKEN_K_GTYPE);
        state = 2779;
        match(TOKEN_DOT);
        state = 2780;
        match(TOKEN_K_CHARU);
        break;
      case 25:
        enterOuterAlt(_localctx, 25);
        state = 2781;
        match(TOKEN_K_DATETIME);
        break;
      case 26:
        enterOuterAlt(_localctx, 26);
        state = 2782;
        match(TOKEN_K_GTYPE);
        state = 2783;
        match(TOKEN_DOT);
        state = 2784;
        match(TOKEN_K_DATETIME);
        break;
      case 27:
        enterOuterAlt(_localctx, 27);
        state = 2785;
        match(TOKEN_K_DATETIMEU);
        break;
      case 28:
        enterOuterAlt(_localctx, 28);
        state = 2786;
        match(TOKEN_K_GTYPE);
        state = 2787;
        match(TOKEN_DOT);
        state = 2788;
        match(TOKEN_K_DATETIMEU);
        break;
      case 29:
        enterOuterAlt(_localctx, 29);
        state = 2789;
        match(TOKEN_K_DOUBLE);
        break;
      case 30:
        enterOuterAlt(_localctx, 30);
        state = 2790;
        match(TOKEN_K_GTYPE);
        state = 2791;
        match(TOKEN_DOT);
        state = 2792;
        match(TOKEN_K_DOUBLE);
        break;
      case 31:
        enterOuterAlt(_localctx, 31);
        state = 2793;
        match(TOKEN_K_DOUBLEU);
        break;
      case 32:
        enterOuterAlt(_localctx, 32);
        state = 2794;
        match(TOKEN_K_GTYPE);
        state = 2795;
        match(TOKEN_DOT);
        state = 2796;
        match(TOKEN_K_DOUBLEU);
        break;
      case 33:
        enterOuterAlt(_localctx, 33);
        state = 2797;
        match(TOKEN_K_DURATION);
        break;
      case 34:
        enterOuterAlt(_localctx, 34);
        state = 2798;
        match(TOKEN_K_GTYPE);
        state = 2799;
        match(TOKEN_DOT);
        state = 2800;
        match(TOKEN_K_DURATION);
        break;
      case 35:
        enterOuterAlt(_localctx, 35);
        state = 2801;
        match(TOKEN_K_DURATIONU);
        break;
      case 36:
        enterOuterAlt(_localctx, 36);
        state = 2802;
        match(TOKEN_K_GTYPE);
        state = 2803;
        match(TOKEN_DOT);
        state = 2804;
        match(TOKEN_K_DURATIONU);
        break;
      case 37:
        enterOuterAlt(_localctx, 37);
        state = 2805;
        match(TOKEN_K_EDGE);
        break;
      case 38:
        enterOuterAlt(_localctx, 38);
        state = 2806;
        match(TOKEN_K_GTYPE);
        state = 2807;
        match(TOKEN_DOT);
        state = 2808;
        match(TOKEN_K_EDGE);
        break;
      case 39:
        enterOuterAlt(_localctx, 39);
        state = 2809;
        match(TOKEN_K_EDGEU);
        break;
      case 40:
        enterOuterAlt(_localctx, 40);
        state = 2810;
        match(TOKEN_K_GTYPE);
        state = 2811;
        match(TOKEN_DOT);
        state = 2812;
        match(TOKEN_K_EDGEU);
        break;
      case 41:
        enterOuterAlt(_localctx, 41);
        state = 2813;
        match(TOKEN_K_FLOAT);
        break;
      case 42:
        enterOuterAlt(_localctx, 42);
        state = 2814;
        match(TOKEN_K_GTYPE);
        state = 2815;
        match(TOKEN_DOT);
        state = 2816;
        match(TOKEN_K_FLOAT);
        break;
      case 43:
        enterOuterAlt(_localctx, 43);
        state = 2817;
        match(TOKEN_K_FLOATU);
        break;
      case 44:
        enterOuterAlt(_localctx, 44);
        state = 2818;
        match(TOKEN_K_GTYPE);
        state = 2819;
        match(TOKEN_DOT);
        state = 2820;
        match(TOKEN_K_FLOATU);
        break;
      case 45:
        enterOuterAlt(_localctx, 45);
        state = 2821;
        match(TOKEN_K_GRAPH);
        break;
      case 46:
        enterOuterAlt(_localctx, 46);
        state = 2822;
        match(TOKEN_K_GTYPE);
        state = 2823;
        match(TOKEN_DOT);
        state = 2824;
        match(TOKEN_K_GRAPH);
        break;
      case 47:
        enterOuterAlt(_localctx, 47);
        state = 2825;
        match(TOKEN_K_GRAPHU);
        break;
      case 48:
        enterOuterAlt(_localctx, 48);
        state = 2826;
        match(TOKEN_K_GTYPE);
        state = 2827;
        match(TOKEN_DOT);
        state = 2828;
        match(TOKEN_K_GRAPHU);
        break;
      case 49:
        enterOuterAlt(_localctx, 49);
        state = 2829;
        match(TOKEN_K_INT);
        break;
      case 50:
        enterOuterAlt(_localctx, 50);
        state = 2830;
        match(TOKEN_K_GTYPE);
        state = 2831;
        match(TOKEN_DOT);
        state = 2832;
        match(TOKEN_K_INT);
        break;
      case 51:
        enterOuterAlt(_localctx, 51);
        state = 2833;
        match(TOKEN_K_INTU);
        break;
      case 52:
        enterOuterAlt(_localctx, 52);
        state = 2834;
        match(TOKEN_K_GTYPE);
        state = 2835;
        match(TOKEN_DOT);
        state = 2836;
        match(TOKEN_K_INTU);
        break;
      case 53:
        enterOuterAlt(_localctx, 53);
        state = 2837;
        match(TOKEN_K_LIST);
        break;
      case 54:
        enterOuterAlt(_localctx, 54);
        state = 2838;
        match(TOKEN_K_GTYPE);
        state = 2839;
        match(TOKEN_DOT);
        state = 2840;
        match(TOKEN_K_LIST);
        break;
      case 55:
        enterOuterAlt(_localctx, 55);
        state = 2841;
        match(TOKEN_K_LISTU);
        break;
      case 56:
        enterOuterAlt(_localctx, 56);
        state = 2842;
        match(TOKEN_K_GTYPE);
        state = 2843;
        match(TOKEN_DOT);
        state = 2844;
        match(TOKEN_K_LISTU);
        break;
      case 57:
        enterOuterAlt(_localctx, 57);
        state = 2845;
        match(TOKEN_K_LONG);
        break;
      case 58:
        enterOuterAlt(_localctx, 58);
        state = 2846;
        match(TOKEN_K_GTYPE);
        state = 2847;
        match(TOKEN_DOT);
        state = 2848;
        match(TOKEN_K_LONG);
        break;
      case 59:
        enterOuterAlt(_localctx, 59);
        state = 2849;
        match(TOKEN_K_LONGU);
        break;
      case 60:
        enterOuterAlt(_localctx, 60);
        state = 2850;
        match(TOKEN_K_GTYPE);
        state = 2851;
        match(TOKEN_DOT);
        state = 2852;
        match(TOKEN_K_LONGU);
        break;
      case 61:
        enterOuterAlt(_localctx, 61);
        state = 2853;
        match(TOKEN_K_MAP);
        break;
      case 62:
        enterOuterAlt(_localctx, 62);
        state = 2854;
        match(TOKEN_K_GTYPE);
        state = 2855;
        match(TOKEN_DOT);
        state = 2856;
        match(TOKEN_K_MAP);
        break;
      case 63:
        enterOuterAlt(_localctx, 63);
        state = 2857;
        match(TOKEN_K_MAPU);
        break;
      case 64:
        enterOuterAlt(_localctx, 64);
        state = 2858;
        match(TOKEN_K_GTYPE);
        state = 2859;
        match(TOKEN_DOT);
        state = 2860;
        match(TOKEN_K_MAPU);
        break;
      case 65:
        enterOuterAlt(_localctx, 65);
        state = 2861;
        match(TOKEN_K_NULL);
        break;
      case 66:
        enterOuterAlt(_localctx, 66);
        state = 2862;
        match(TOKEN_K_GTYPE);
        state = 2863;
        match(TOKEN_DOT);
        state = 2864;
        match(TOKEN_K_NULL);
        break;
      case 67:
        enterOuterAlt(_localctx, 67);
        state = 2865;
        match(TOKEN_K_NULLU);
        break;
      case 68:
        enterOuterAlt(_localctx, 68);
        state = 2866;
        match(TOKEN_K_GTYPE);
        state = 2867;
        match(TOKEN_DOT);
        state = 2868;
        match(TOKEN_K_NULLU);
        break;
      case 69:
        enterOuterAlt(_localctx, 69);
        state = 2869;
        match(TOKEN_K_NUMBER);
        break;
      case 70:
        enterOuterAlt(_localctx, 70);
        state = 2870;
        match(TOKEN_K_GTYPE);
        state = 2871;
        match(TOKEN_DOT);
        state = 2872;
        match(TOKEN_K_NUMBER);
        break;
      case 71:
        enterOuterAlt(_localctx, 71);
        state = 2873;
        match(TOKEN_K_NUMBERU);
        break;
      case 72:
        enterOuterAlt(_localctx, 72);
        state = 2874;
        match(TOKEN_K_GTYPE);
        state = 2875;
        match(TOKEN_DOT);
        state = 2876;
        match(TOKEN_K_NUMBERU);
        break;
      case 73:
        enterOuterAlt(_localctx, 73);
        state = 2877;
        match(TOKEN_K_PATH);
        break;
      case 74:
        enterOuterAlt(_localctx, 74);
        state = 2878;
        match(TOKEN_K_GTYPE);
        state = 2879;
        match(TOKEN_DOT);
        state = 2880;
        match(TOKEN_K_PATH);
        break;
      case 75:
        enterOuterAlt(_localctx, 75);
        state = 2881;
        match(TOKEN_K_PATHU);
        break;
      case 76:
        enterOuterAlt(_localctx, 76);
        state = 2882;
        match(TOKEN_K_GTYPE);
        state = 2883;
        match(TOKEN_DOT);
        state = 2884;
        match(TOKEN_K_PATHU);
        break;
      case 77:
        enterOuterAlt(_localctx, 77);
        state = 2885;
        match(TOKEN_K_PROPERTY);
        break;
      case 78:
        enterOuterAlt(_localctx, 78);
        state = 2886;
        match(TOKEN_K_GTYPE);
        state = 2887;
        match(TOKEN_DOT);
        state = 2888;
        match(TOKEN_K_PROPERTY);
        break;
      case 79:
        enterOuterAlt(_localctx, 79);
        state = 2889;
        match(TOKEN_K_PROPERTYU);
        break;
      case 80:
        enterOuterAlt(_localctx, 80);
        state = 2890;
        match(TOKEN_K_GTYPE);
        state = 2891;
        match(TOKEN_DOT);
        state = 2892;
        match(TOKEN_K_PROPERTYU);
        break;
      case 81:
        enterOuterAlt(_localctx, 81);
        state = 2893;
        match(TOKEN_K_SET);
        break;
      case 82:
        enterOuterAlt(_localctx, 82);
        state = 2894;
        match(TOKEN_K_GTYPE);
        state = 2895;
        match(TOKEN_DOT);
        state = 2896;
        match(TOKEN_K_SET);
        break;
      case 83:
        enterOuterAlt(_localctx, 83);
        state = 2897;
        match(TOKEN_K_SETU);
        break;
      case 84:
        enterOuterAlt(_localctx, 84);
        state = 2898;
        match(TOKEN_K_GTYPE);
        state = 2899;
        match(TOKEN_DOT);
        state = 2900;
        match(TOKEN_K_SETU);
        break;
      case 85:
        enterOuterAlt(_localctx, 85);
        state = 2901;
        match(TOKEN_K_SHORT);
        break;
      case 86:
        enterOuterAlt(_localctx, 86);
        state = 2902;
        match(TOKEN_K_GTYPE);
        state = 2903;
        match(TOKEN_DOT);
        state = 2904;
        match(TOKEN_K_SHORT);
        break;
      case 87:
        enterOuterAlt(_localctx, 87);
        state = 2905;
        match(TOKEN_K_SHORTU);
        break;
      case 88:
        enterOuterAlt(_localctx, 88);
        state = 2906;
        match(TOKEN_K_GTYPE);
        state = 2907;
        match(TOKEN_DOT);
        state = 2908;
        match(TOKEN_K_SHORTU);
        break;
      case 89:
        enterOuterAlt(_localctx, 89);
        state = 2909;
        match(TOKEN_K_STRING);
        break;
      case 90:
        enterOuterAlt(_localctx, 90);
        state = 2910;
        match(TOKEN_K_GTYPE);
        state = 2911;
        match(TOKEN_DOT);
        state = 2912;
        match(TOKEN_K_STRING);
        break;
      case 91:
        enterOuterAlt(_localctx, 91);
        state = 2913;
        match(TOKEN_K_STRINGU);
        break;
      case 92:
        enterOuterAlt(_localctx, 92);
        state = 2914;
        match(TOKEN_K_GTYPE);
        state = 2915;
        match(TOKEN_DOT);
        state = 2916;
        match(TOKEN_K_STRINGU);
        break;
      case 93:
        enterOuterAlt(_localctx, 93);
        state = 2917;
        match(TOKEN_K_TREE);
        break;
      case 94:
        enterOuterAlt(_localctx, 94);
        state = 2918;
        match(TOKEN_K_GTYPE);
        state = 2919;
        match(TOKEN_DOT);
        state = 2920;
        match(TOKEN_K_TREE);
        break;
      case 95:
        enterOuterAlt(_localctx, 95);
        state = 2921;
        match(TOKEN_K_TREEU);
        break;
      case 96:
        enterOuterAlt(_localctx, 96);
        state = 2922;
        match(TOKEN_K_GTYPE);
        state = 2923;
        match(TOKEN_DOT);
        state = 2924;
        match(TOKEN_K_TREEU);
        break;
      case 97:
        enterOuterAlt(_localctx, 97);
        state = 2925;
        match(TOKEN_K_UUID);
        break;
      case 98:
        enterOuterAlt(_localctx, 98);
        state = 2926;
        match(TOKEN_K_GTYPE);
        state = 2927;
        match(TOKEN_DOT);
        state = 2928;
        match(TOKEN_K_UUID);
        break;
      case 99:
        enterOuterAlt(_localctx, 99);
        state = 2929;
        match(TOKEN_K_UUIDL);
        break;
      case 100:
        enterOuterAlt(_localctx, 100);
        state = 2930;
        match(TOKEN_K_GTYPE);
        state = 2931;
        match(TOKEN_DOT);
        state = 2932;
        match(TOKEN_K_UUIDL);
        break;
      case 101:
        enterOuterAlt(_localctx, 101);
        state = 2933;
        match(TOKEN_K_VERTEX);
        break;
      case 102:
        enterOuterAlt(_localctx, 102);
        state = 2934;
        match(TOKEN_K_GTYPE);
        state = 2935;
        match(TOKEN_DOT);
        state = 2936;
        match(TOKEN_K_VERTEX);
        break;
      case 103:
        enterOuterAlt(_localctx, 103);
        state = 2937;
        match(TOKEN_K_VERTEXU);
        break;
      case 104:
        enterOuterAlt(_localctx, 104);
        state = 2938;
        match(TOKEN_K_GTYPE);
        state = 2939;
        match(TOKEN_DOT);
        state = 2940;
        match(TOKEN_K_VERTEXU);
        break;
      case 105:
        enterOuterAlt(_localctx, 105);
        state = 2941;
        match(TOKEN_K_VPROPERTY);
        break;
      case 106:
        enterOuterAlt(_localctx, 106);
        state = 2942;
        match(TOKEN_K_GTYPE);
        state = 2943;
        match(TOKEN_DOT);
        state = 2944;
        match(TOKEN_K_VPROPERTY);
        break;
      case 107:
        enterOuterAlt(_localctx, 107);
        state = 2945;
        match(TOKEN_K_VPROPERTYU);
        break;
      case 108:
        enterOuterAlt(_localctx, 108);
        state = 2946;
        match(TOKEN_K_GTYPE);
        state = 2947;
        match(TOKEN_DOT);
        state = 2948;
        match(TOKEN_K_VPROPERTYU);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicateContext traversalPredicate([int _p = 0]) {
    final _parentctx = context;
    final _parentState = state;
    dynamic _localctx = TraversalPredicateContext(context, _parentState);
    var _prevctx = _localctx;
    var _startState = 366;
    enterRecursionRule(_localctx, 366, RULE_traversalPredicate, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      state = 2973;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 126, context)) {
      case 1:
        state = 2952;
        traversalPredicate_eq();
        break;
      case 2:
        state = 2953;
        traversalPredicate_neq();
        break;
      case 3:
        state = 2954;
        traversalPredicate_lt();
        break;
      case 4:
        state = 2955;
        traversalPredicate_lte();
        break;
      case 5:
        state = 2956;
        traversalPredicate_gt();
        break;
      case 6:
        state = 2957;
        traversalPredicate_gte();
        break;
      case 7:
        state = 2958;
        traversalPredicate_inside();
        break;
      case 8:
        state = 2959;
        traversalPredicate_outside();
        break;
      case 9:
        state = 2960;
        traversalPredicate_between();
        break;
      case 10:
        state = 2961;
        traversalPredicate_typeOf();
        break;
      case 11:
        state = 2962;
        traversalPredicate_within();
        break;
      case 12:
        state = 2963;
        traversalPredicate_without();
        break;
      case 13:
        state = 2964;
        traversalPredicate_not();
        break;
      case 14:
        state = 2965;
        traversalPredicate_startingWith();
        break;
      case 15:
        state = 2966;
        traversalPredicate_notStartingWith();
        break;
      case 16:
        state = 2967;
        traversalPredicate_endingWith();
        break;
      case 17:
        state = 2968;
        traversalPredicate_notEndingWith();
        break;
      case 18:
        state = 2969;
        traversalPredicate_containing();
        break;
      case 19:
        state = 2970;
        traversalPredicate_notContaining();
        break;
      case 20:
        state = 2971;
        traversalPredicate_regex();
        break;
      case 21:
        state = 2972;
        traversalPredicate_notRegex();
        break;
      }
      context!.stop = tokenStream.LT(-1);
      state = 2996;
      errorHandler.sync(this);
      _alt = interpreter!.adaptivePredict(tokenStream, 128, context);
      while (_alt != 2 && _alt != ATN.INVALID_ALT_NUMBER) {
        if (_alt == 1) {
          if (parseListeners != null) triggerExitRuleEvent();
          _prevctx = _localctx;
          state = 2994;
          errorHandler.sync(this);
          switch (interpreter!.adaptivePredict(tokenStream, 127, context)) {
          case 1:
            _localctx = TraversalPredicateContext(_parentctx, _parentState);
            pushNewRecursionContext(_localctx, _startState, RULE_traversalPredicate);
            state = 2975;
            if (!(precpred(context, 3))) {
              throw FailedPredicateException(this, "precpred(context, 3)");
            }
            state = 2976;
            match(TOKEN_DOT);
            state = 2977;
            match(TOKEN_K_AND);
            state = 2978;
            match(TOKEN_LPAREN);
            state = 2979;
            traversalPredicate(0);
            state = 2980;
            match(TOKEN_RPAREN);
            break;
          case 2:
            _localctx = TraversalPredicateContext(_parentctx, _parentState);
            pushNewRecursionContext(_localctx, _startState, RULE_traversalPredicate);
            state = 2982;
            if (!(precpred(context, 2))) {
              throw FailedPredicateException(this, "precpred(context, 2)");
            }
            state = 2983;
            match(TOKEN_DOT);
            state = 2984;
            match(TOKEN_K_OR);
            state = 2985;
            match(TOKEN_LPAREN);
            state = 2986;
            traversalPredicate(0);
            state = 2987;
            match(TOKEN_RPAREN);
            break;
          case 3:
            _localctx = TraversalPredicateContext(_parentctx, _parentState);
            pushNewRecursionContext(_localctx, _startState, RULE_traversalPredicate);
            state = 2989;
            if (!(precpred(context, 1))) {
              throw FailedPredicateException(this, "precpred(context, 1)");
            }
            state = 2990;
            match(TOKEN_DOT);
            state = 2991;
            match(TOKEN_K_NEGATE);
            state = 2992;
            match(TOKEN_LPAREN);
            state = 2993;
            match(TOKEN_RPAREN);
            break;
          } 
        }
        state = 2998;
        errorHandler.sync(this);
        _alt = interpreter!.adaptivePredict(tokenStream, 128, context);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  TraversalTerminalMethodContext traversalTerminalMethod() {
    dynamic _localctx = TraversalTerminalMethodContext(context, state);
    enterRule(_localctx, 368, RULE_traversalTerminalMethod);
    try {
      state = 3007;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_EXPLAIN:
        enterOuterAlt(_localctx, 1);
        state = 2999;
        traversalTerminalMethod_explain();
        break;
      case TOKEN_K_ITERATE:
        enterOuterAlt(_localctx, 2);
        state = 3000;
        traversalTerminalMethod_iterate();
        break;
      case TOKEN_K_HASNEXT:
        enterOuterAlt(_localctx, 3);
        state = 3001;
        traversalTerminalMethod_hasNext();
        break;
      case TOKEN_K_TRYNEXT:
        enterOuterAlt(_localctx, 4);
        state = 3002;
        traversalTerminalMethod_tryNext();
        break;
      case TOKEN_K_NEXT:
        enterOuterAlt(_localctx, 5);
        state = 3003;
        traversalTerminalMethod_next();
        break;
      case TOKEN_K_TOLIST:
        enterOuterAlt(_localctx, 6);
        state = 3004;
        traversalTerminalMethod_toList();
        break;
      case TOKEN_K_TOSET:
        enterOuterAlt(_localctx, 7);
        state = 3005;
        traversalTerminalMethod_toSet();
        break;
      case TOKEN_K_TOBULKSET:
        enterOuterAlt(_localctx, 8);
        state = 3006;
        traversalTerminalMethod_toBulkSet();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalSackMethodContext traversalSackMethod() {
    dynamic _localctx = TraversalSackMethodContext(context, state);
    enterRule(_localctx, 370, RULE_traversalSackMethod);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3009;
      traversalBarrier();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalComparatorContext traversalComparator() {
    dynamic _localctx = TraversalComparatorContext(context, state);
    enterRule(_localctx, 372, RULE_traversalComparator);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3011;
      traversalOrder();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalFunctionContext traversalFunction() {
    dynamic _localctx = TraversalFunctionContext(context, state);
    enterRule(_localctx, 374, RULE_traversalFunction);
    try {
      state = 3015;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ID:
      case TOKEN_K_KEY:
      case TOKEN_K_LABEL:
      case TOKEN_K_T:
      case TOKEN_K_VALUE:
        enterOuterAlt(_localctx, 1);
        state = 3013;
        traversalT();
        break;
      case TOKEN_K_COLUMN:
      case TOKEN_K_KEYS:
      case TOKEN_K_VALUES:
        enterOuterAlt(_localctx, 2);
        state = 3014;
        traversalColumn();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalBiFunctionContext traversalBiFunction() {
    dynamic _localctx = TraversalBiFunctionContext(context, state);
    enterRule(_localctx, 376, RULE_traversalBiFunction);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3017;
      traversalOperator();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_eqContext traversalPredicate_eq() {
    dynamic _localctx = TraversalPredicate_eqContext(context, state);
    enterRule(_localctx, 378, RULE_traversalPredicate_eq);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3023;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3019;
        match(TOKEN_K_P);
        state = 3020;
        match(TOKEN_DOT);
        state = 3021;
        match(TOKEN_K_EQ);
        break;
      case TOKEN_K_EQ:
        state = 3022;
        match(TOKEN_K_EQ);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3025;
      match(TOKEN_LPAREN);
      state = 3026;
      genericArgument();
      state = 3027;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_neqContext traversalPredicate_neq() {
    dynamic _localctx = TraversalPredicate_neqContext(context, state);
    enterRule(_localctx, 380, RULE_traversalPredicate_neq);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3033;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3029;
        match(TOKEN_K_P);
        state = 3030;
        match(TOKEN_DOT);
        state = 3031;
        match(TOKEN_K_NEQ);
        break;
      case TOKEN_K_NEQ:
        state = 3032;
        match(TOKEN_K_NEQ);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3035;
      match(TOKEN_LPAREN);
      state = 3036;
      genericArgument();
      state = 3037;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_typeOfContext traversalPredicate_typeOf() {
    dynamic _localctx = TraversalPredicate_typeOfContext(context, state);
    enterRule(_localctx, 382, RULE_traversalPredicate_typeOf);
    try {
      state = 3059;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 135, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3043;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3039;
          match(TOKEN_K_P);
          state = 3040;
          match(TOKEN_DOT);
          state = 3041;
          match(TOKEN_K_TYPEOF);
          break;
        case TOKEN_K_TYPEOF:
          state = 3042;
          match(TOKEN_K_TYPEOF);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3045;
        match(TOKEN_LPAREN);
        state = 3046;
        traversalGType();
        state = 3047;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3053;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3049;
          match(TOKEN_K_P);
          state = 3050;
          match(TOKEN_DOT);
          state = 3051;
          match(TOKEN_K_TYPEOF);
          break;
        case TOKEN_K_TYPEOF:
          state = 3052;
          match(TOKEN_K_TYPEOF);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3055;
        match(TOKEN_LPAREN);
        state = 3056;
        stringLiteral();
        state = 3057;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_ltContext traversalPredicate_lt() {
    dynamic _localctx = TraversalPredicate_ltContext(context, state);
    enterRule(_localctx, 384, RULE_traversalPredicate_lt);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3065;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3061;
        match(TOKEN_K_P);
        state = 3062;
        match(TOKEN_DOT);
        state = 3063;
        match(TOKEN_K_LT);
        break;
      case TOKEN_K_LT:
        state = 3064;
        match(TOKEN_K_LT);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3067;
      match(TOKEN_LPAREN);
      state = 3068;
      genericArgument();
      state = 3069;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_lteContext traversalPredicate_lte() {
    dynamic _localctx = TraversalPredicate_lteContext(context, state);
    enterRule(_localctx, 386, RULE_traversalPredicate_lte);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3075;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3071;
        match(TOKEN_K_P);
        state = 3072;
        match(TOKEN_DOT);
        state = 3073;
        match(TOKEN_K_LTE);
        break;
      case TOKEN_K_LTE:
        state = 3074;
        match(TOKEN_K_LTE);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3077;
      match(TOKEN_LPAREN);
      state = 3078;
      genericArgument();
      state = 3079;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_gtContext traversalPredicate_gt() {
    dynamic _localctx = TraversalPredicate_gtContext(context, state);
    enterRule(_localctx, 388, RULE_traversalPredicate_gt);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3085;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3081;
        match(TOKEN_K_P);
        state = 3082;
        match(TOKEN_DOT);
        state = 3083;
        match(TOKEN_K_GT);
        break;
      case TOKEN_K_GT:
        state = 3084;
        match(TOKEN_K_GT);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3087;
      match(TOKEN_LPAREN);
      state = 3088;
      genericArgument();
      state = 3089;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_gteContext traversalPredicate_gte() {
    dynamic _localctx = TraversalPredicate_gteContext(context, state);
    enterRule(_localctx, 390, RULE_traversalPredicate_gte);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3095;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3091;
        match(TOKEN_K_P);
        state = 3092;
        match(TOKEN_DOT);
        state = 3093;
        match(TOKEN_K_GTE);
        break;
      case TOKEN_K_GTE:
        state = 3094;
        match(TOKEN_K_GTE);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3097;
      match(TOKEN_LPAREN);
      state = 3098;
      genericArgument();
      state = 3099;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_insideContext traversalPredicate_inside() {
    dynamic _localctx = TraversalPredicate_insideContext(context, state);
    enterRule(_localctx, 392, RULE_traversalPredicate_inside);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3105;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3101;
        match(TOKEN_K_P);
        state = 3102;
        match(TOKEN_DOT);
        state = 3103;
        match(TOKEN_K_INSIDE);
        break;
      case TOKEN_K_INSIDE:
        state = 3104;
        match(TOKEN_K_INSIDE);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3107;
      match(TOKEN_LPAREN);
      state = 3108;
      genericArgument();
      state = 3109;
      match(TOKEN_COMMA);
      state = 3110;
      genericArgument();
      state = 3111;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_outsideContext traversalPredicate_outside() {
    dynamic _localctx = TraversalPredicate_outsideContext(context, state);
    enterRule(_localctx, 394, RULE_traversalPredicate_outside);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3117;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3113;
        match(TOKEN_K_P);
        state = 3114;
        match(TOKEN_DOT);
        state = 3115;
        match(TOKEN_K_OUTSIDE);
        break;
      case TOKEN_K_OUTSIDE:
        state = 3116;
        match(TOKEN_K_OUTSIDE);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3119;
      match(TOKEN_LPAREN);
      state = 3120;
      genericArgument();
      state = 3121;
      match(TOKEN_COMMA);
      state = 3122;
      genericArgument();
      state = 3123;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_betweenContext traversalPredicate_between() {
    dynamic _localctx = TraversalPredicate_betweenContext(context, state);
    enterRule(_localctx, 396, RULE_traversalPredicate_between);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3129;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3125;
        match(TOKEN_K_P);
        state = 3126;
        match(TOKEN_DOT);
        state = 3127;
        match(TOKEN_K_BETWEEN);
        break;
      case TOKEN_K_BETWEEN:
        state = 3128;
        match(TOKEN_K_BETWEEN);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3131;
      match(TOKEN_LPAREN);
      state = 3132;
      genericArgument();
      state = 3133;
      match(TOKEN_COMMA);
      state = 3134;
      genericArgument();
      state = 3135;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_withinContext traversalPredicate_within() {
    dynamic _localctx = TraversalPredicate_withinContext(context, state);
    enterRule(_localctx, 398, RULE_traversalPredicate_within);
    try {
      state = 3155;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 145, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3141;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3137;
          match(TOKEN_K_P);
          state = 3138;
          match(TOKEN_DOT);
          state = 3139;
          match(TOKEN_K_WITHIN);
          break;
        case TOKEN_K_WITHIN:
          state = 3140;
          match(TOKEN_K_WITHIN);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3143;
        match(TOKEN_LPAREN);
        state = 3144;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3149;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3145;
          match(TOKEN_K_P);
          state = 3146;
          match(TOKEN_DOT);
          state = 3147;
          match(TOKEN_K_WITHIN);
          break;
        case TOKEN_K_WITHIN:
          state = 3148;
          match(TOKEN_K_WITHIN);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3151;
        match(TOKEN_LPAREN);
        state = 3152;
        genericArgumentVarargs();
        state = 3153;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_withoutContext traversalPredicate_without() {
    dynamic _localctx = TraversalPredicate_withoutContext(context, state);
    enterRule(_localctx, 400, RULE_traversalPredicate_without);
    try {
      state = 3175;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 148, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3161;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3157;
          match(TOKEN_K_P);
          state = 3158;
          match(TOKEN_DOT);
          state = 3159;
          match(TOKEN_K_WITHOUT);
          break;
        case TOKEN_K_WITHOUT:
          state = 3160;
          match(TOKEN_K_WITHOUT);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3163;
        match(TOKEN_LPAREN);
        state = 3164;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3169;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_P:
          state = 3165;
          match(TOKEN_K_P);
          state = 3166;
          match(TOKEN_DOT);
          state = 3167;
          match(TOKEN_K_WITHOUT);
          break;
        case TOKEN_K_WITHOUT:
          state = 3168;
          match(TOKEN_K_WITHOUT);
          break;
        default:
          throw NoViableAltException(this);
        }
        state = 3171;
        match(TOKEN_LPAREN);
        state = 3172;
        genericArgumentVarargs();
        state = 3173;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_notContext traversalPredicate_not() {
    dynamic _localctx = TraversalPredicate_notContext(context, state);
    enterRule(_localctx, 402, RULE_traversalPredicate_not);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3181;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_P:
        state = 3177;
        match(TOKEN_K_P);
        state = 3178;
        match(TOKEN_DOT);
        state = 3179;
        match(TOKEN_K_NOT);
        break;
      case TOKEN_K_NOT:
        state = 3180;
        match(TOKEN_K_NOT);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3183;
      match(TOKEN_LPAREN);
      state = 3184;
      traversalPredicate(0);
      state = 3185;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_containingContext traversalPredicate_containing() {
    dynamic _localctx = TraversalPredicate_containingContext(context, state);
    enterRule(_localctx, 404, RULE_traversalPredicate_containing);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3191;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3187;
        match(TOKEN_K_TEXTP);
        state = 3188;
        match(TOKEN_DOT);
        state = 3189;
        match(TOKEN_K_CONTAINING);
        break;
      case TOKEN_K_CONTAINING:
        state = 3190;
        match(TOKEN_K_CONTAINING);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3193;
      match(TOKEN_LPAREN);
      state = 3194;
      stringArgument();
      state = 3195;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_notContainingContext traversalPredicate_notContaining() {
    dynamic _localctx = TraversalPredicate_notContainingContext(context, state);
    enterRule(_localctx, 406, RULE_traversalPredicate_notContaining);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3201;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3197;
        match(TOKEN_K_TEXTP);
        state = 3198;
        match(TOKEN_DOT);
        state = 3199;
        match(TOKEN_K_NOTCONTAINING);
        break;
      case TOKEN_K_NOTCONTAINING:
        state = 3200;
        match(TOKEN_K_NOTCONTAINING);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3203;
      match(TOKEN_LPAREN);
      state = 3204;
      stringArgument();
      state = 3205;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_startingWithContext traversalPredicate_startingWith() {
    dynamic _localctx = TraversalPredicate_startingWithContext(context, state);
    enterRule(_localctx, 408, RULE_traversalPredicate_startingWith);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3211;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3207;
        match(TOKEN_K_TEXTP);
        state = 3208;
        match(TOKEN_DOT);
        state = 3209;
        match(TOKEN_K_STARTINGWITH);
        break;
      case TOKEN_K_STARTINGWITH:
        state = 3210;
        match(TOKEN_K_STARTINGWITH);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3213;
      match(TOKEN_LPAREN);
      state = 3214;
      stringArgument();
      state = 3215;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_notStartingWithContext traversalPredicate_notStartingWith() {
    dynamic _localctx = TraversalPredicate_notStartingWithContext(context, state);
    enterRule(_localctx, 410, RULE_traversalPredicate_notStartingWith);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3221;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3217;
        match(TOKEN_K_TEXTP);
        state = 3218;
        match(TOKEN_DOT);
        state = 3219;
        match(TOKEN_K_NOTSTARTINGWITH);
        break;
      case TOKEN_K_NOTSTARTINGWITH:
        state = 3220;
        match(TOKEN_K_NOTSTARTINGWITH);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3223;
      match(TOKEN_LPAREN);
      state = 3224;
      stringArgument();
      state = 3225;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_endingWithContext traversalPredicate_endingWith() {
    dynamic _localctx = TraversalPredicate_endingWithContext(context, state);
    enterRule(_localctx, 412, RULE_traversalPredicate_endingWith);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3231;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3227;
        match(TOKEN_K_TEXTP);
        state = 3228;
        match(TOKEN_DOT);
        state = 3229;
        match(TOKEN_K_ENDINGWITH);
        break;
      case TOKEN_K_ENDINGWITH:
        state = 3230;
        match(TOKEN_K_ENDINGWITH);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3233;
      match(TOKEN_LPAREN);
      state = 3234;
      stringArgument();
      state = 3235;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_notEndingWithContext traversalPredicate_notEndingWith() {
    dynamic _localctx = TraversalPredicate_notEndingWithContext(context, state);
    enterRule(_localctx, 414, RULE_traversalPredicate_notEndingWith);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3241;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3237;
        match(TOKEN_K_TEXTP);
        state = 3238;
        match(TOKEN_DOT);
        state = 3239;
        match(TOKEN_K_NOTENDINGWITH);
        break;
      case TOKEN_K_NOTENDINGWITH:
        state = 3240;
        match(TOKEN_K_NOTENDINGWITH);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3243;
      match(TOKEN_LPAREN);
      state = 3244;
      stringArgument();
      state = 3245;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_regexContext traversalPredicate_regex() {
    dynamic _localctx = TraversalPredicate_regexContext(context, state);
    enterRule(_localctx, 416, RULE_traversalPredicate_regex);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3251;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3247;
        match(TOKEN_K_TEXTP);
        state = 3248;
        match(TOKEN_DOT);
        state = 3249;
        match(TOKEN_K_REGEX);
        break;
      case TOKEN_K_REGEX:
        state = 3250;
        match(TOKEN_K_REGEX);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3253;
      match(TOKEN_LPAREN);
      state = 3254;
      stringArgument();
      state = 3255;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalPredicate_notRegexContext traversalPredicate_notRegex() {
    dynamic _localctx = TraversalPredicate_notRegexContext(context, state);
    enterRule(_localctx, 418, RULE_traversalPredicate_notRegex);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3261;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_TEXTP:
        state = 3257;
        match(TOKEN_K_TEXTP);
        state = 3258;
        match(TOKEN_DOT);
        state = 3259;
        match(TOKEN_K_NOTREGEX);
        break;
      case TOKEN_K_NOTREGEX:
        state = 3260;
        match(TOKEN_K_NOTREGEX);
        break;
      default:
        throw NoViableAltException(this);
      }
      state = 3263;
      match(TOKEN_LPAREN);
      state = 3264;
      stringArgument();
      state = 3265;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_explainContext traversalTerminalMethod_explain() {
    dynamic _localctx = TraversalTerminalMethod_explainContext(context, state);
    enterRule(_localctx, 420, RULE_traversalTerminalMethod_explain);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3267;
      match(TOKEN_K_EXPLAIN);
      state = 3268;
      match(TOKEN_LPAREN);
      state = 3269;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_hasNextContext traversalTerminalMethod_hasNext() {
    dynamic _localctx = TraversalTerminalMethod_hasNextContext(context, state);
    enterRule(_localctx, 422, RULE_traversalTerminalMethod_hasNext);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3271;
      match(TOKEN_K_HASNEXT);
      state = 3272;
      match(TOKEN_LPAREN);
      state = 3273;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_iterateContext traversalTerminalMethod_iterate() {
    dynamic _localctx = TraversalTerminalMethod_iterateContext(context, state);
    enterRule(_localctx, 424, RULE_traversalTerminalMethod_iterate);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3275;
      match(TOKEN_K_ITERATE);
      state = 3276;
      match(TOKEN_LPAREN);
      state = 3277;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_tryNextContext traversalTerminalMethod_tryNext() {
    dynamic _localctx = TraversalTerminalMethod_tryNextContext(context, state);
    enterRule(_localctx, 426, RULE_traversalTerminalMethod_tryNext);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3279;
      match(TOKEN_K_TRYNEXT);
      state = 3280;
      match(TOKEN_LPAREN);
      state = 3281;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_nextContext traversalTerminalMethod_next() {
    dynamic _localctx = TraversalTerminalMethod_nextContext(context, state);
    enterRule(_localctx, 428, RULE_traversalTerminalMethod_next);
    try {
      state = 3291;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 158, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3283;
        match(TOKEN_K_NEXT);
        state = 3284;
        match(TOKEN_LPAREN);
        state = 3285;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3286;
        match(TOKEN_K_NEXT);
        state = 3287;
        match(TOKEN_LPAREN);
        state = 3288;
        integerLiteral();
        state = 3289;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_toListContext traversalTerminalMethod_toList() {
    dynamic _localctx = TraversalTerminalMethod_toListContext(context, state);
    enterRule(_localctx, 430, RULE_traversalTerminalMethod_toList);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3293;
      match(TOKEN_K_TOLIST);
      state = 3294;
      match(TOKEN_LPAREN);
      state = 3295;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_toSetContext traversalTerminalMethod_toSet() {
    dynamic _localctx = TraversalTerminalMethod_toSetContext(context, state);
    enterRule(_localctx, 432, RULE_traversalTerminalMethod_toSet);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3297;
      match(TOKEN_K_TOSET);
      state = 3298;
      match(TOKEN_LPAREN);
      state = 3299;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalTerminalMethod_toBulkSetContext traversalTerminalMethod_toBulkSet() {
    dynamic _localctx = TraversalTerminalMethod_toBulkSetContext(context, state);
    enterRule(_localctx, 434, RULE_traversalTerminalMethod_toBulkSet);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3301;
      match(TOKEN_K_TOBULKSET);
      state = 3302;
      match(TOKEN_LPAREN);
      state = 3303;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionKeysContext withOptionKeys() {
    dynamic _localctx = WithOptionKeysContext(context, state);
    enterRule(_localctx, 436, RULE_withOptionKeys);
    try {
      state = 3312;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 159, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3305;
        shortestPathConstants();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3306;
        connectedComponentConstants();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3307;
        pageRankConstants();
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3308;
        peerPressureConstants();
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 3309;
        ioOptionsKeys();
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 3310;
        withOptionsConstants_tokens();
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 3311;
        withOptionsConstants_indexer();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConnectedComponentConstantsContext connectedComponentConstants() {
    dynamic _localctx = ConnectedComponentConstantsContext(context, state);
    enterRule(_localctx, 438, RULE_connectedComponentConstants);
    try {
      state = 3317;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 160, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3314;
        connectedComponentConstants_component();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3315;
        connectedComponentConstants_edges();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3316;
        connectedComponentConstants_propertyName();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PageRankConstantsContext pageRankConstants() {
    dynamic _localctx = PageRankConstantsContext(context, state);
    enterRule(_localctx, 440, RULE_pageRankConstants);
    try {
      state = 3322;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 161, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3319;
        pageRankConstants_edges();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3320;
        pageRankConstants_times();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3321;
        pageRankConstants_propertyName();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PeerPressureConstantsContext peerPressureConstants() {
    dynamic _localctx = PeerPressureConstantsContext(context, state);
    enterRule(_localctx, 442, RULE_peerPressureConstants);
    try {
      state = 3327;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 162, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3324;
        peerPressureConstants_edges();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3325;
        peerPressureConstants_times();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3326;
        peerPressureConstants_propertyName();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstantsContext shortestPathConstants() {
    dynamic _localctx = ShortestPathConstantsContext(context, state);
    enterRule(_localctx, 444, RULE_shortestPathConstants);
    try {
      state = 3334;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 163, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3329;
        shortestPathConstants_target();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3330;
        shortestPathConstants_edges();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3331;
        shortestPathConstants_distance();
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3332;
        shortestPathConstants_maxDistance();
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 3333;
        shortestPathConstants_includeEdges();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsValuesContext withOptionsValues() {
    dynamic _localctx = WithOptionsValuesContext(context, state);
    enterRule(_localctx, 446, RULE_withOptionsValues);
    try {
      state = 3345;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 164, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3336;
        withOptionsConstants_tokens();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3337;
        withOptionsConstants_none();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3338;
        withOptionsConstants_ids();
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3339;
        withOptionsConstants_labels();
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 3340;
        withOptionsConstants_keys();
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 3341;
        withOptionsConstants_values();
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 3342;
        withOptionsConstants_all();
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 3343;
        withOptionsConstants_list();
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        state = 3344;
        withOptionsConstants_map();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsKeysContext ioOptionsKeys() {
    dynamic _localctx = IoOptionsKeysContext(context, state);
    enterRule(_localctx, 448, RULE_ioOptionsKeys);
    try {
      state = 3349;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 165, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3347;
        ioOptionsConstants_reader();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3348;
        ioOptionsConstants_writer();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsValuesContext ioOptionsValues() {
    dynamic _localctx = IoOptionsValuesContext(context, state);
    enterRule(_localctx, 450, RULE_ioOptionsValues);
    try {
      state = 3354;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 166, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3351;
        ioOptionsConstants_gryo();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3352;
        ioOptionsConstants_graphson();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3353;
        ioOptionsConstants_graphml();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConnectedComponentConstants_componentContext connectedComponentConstants_component() {
    dynamic _localctx = ConnectedComponentConstants_componentContext(context, state);
    enterRule(_localctx, 452, RULE_connectedComponentConstants_component);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3356;
      connectedComponentStringConstant();
      state = 3357;
      match(TOKEN_DOT);
      state = 3358;
      match(TOKEN_K_COMPONENT);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConnectedComponentConstants_edgesContext connectedComponentConstants_edges() {
    dynamic _localctx = ConnectedComponentConstants_edgesContext(context, state);
    enterRule(_localctx, 454, RULE_connectedComponentConstants_edges);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3360;
      connectedComponentStringConstant();
      state = 3361;
      match(TOKEN_DOT);
      state = 3362;
      match(TOKEN_K_EDGES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConnectedComponentConstants_propertyNameContext connectedComponentConstants_propertyName() {
    dynamic _localctx = ConnectedComponentConstants_propertyNameContext(context, state);
    enterRule(_localctx, 456, RULE_connectedComponentConstants_propertyName);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3364;
      connectedComponentStringConstant();
      state = 3365;
      match(TOKEN_DOT);
      state = 3366;
      match(TOKEN_K_PROPERTYNAME);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PageRankConstants_edgesContext pageRankConstants_edges() {
    dynamic _localctx = PageRankConstants_edgesContext(context, state);
    enterRule(_localctx, 458, RULE_pageRankConstants_edges);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3368;
      pageRankStringConstant();
      state = 3369;
      match(TOKEN_DOT);
      state = 3370;
      match(TOKEN_K_EDGES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PageRankConstants_timesContext pageRankConstants_times() {
    dynamic _localctx = PageRankConstants_timesContext(context, state);
    enterRule(_localctx, 460, RULE_pageRankConstants_times);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3372;
      pageRankStringConstant();
      state = 3373;
      match(TOKEN_DOT);
      state = 3374;
      match(TOKEN_K_TIMES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PageRankConstants_propertyNameContext pageRankConstants_propertyName() {
    dynamic _localctx = PageRankConstants_propertyNameContext(context, state);
    enterRule(_localctx, 462, RULE_pageRankConstants_propertyName);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3376;
      pageRankStringConstant();
      state = 3377;
      match(TOKEN_DOT);
      state = 3378;
      match(TOKEN_K_PROPERTYNAME);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PeerPressureConstants_edgesContext peerPressureConstants_edges() {
    dynamic _localctx = PeerPressureConstants_edgesContext(context, state);
    enterRule(_localctx, 464, RULE_peerPressureConstants_edges);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3380;
      peerPressureStringConstant();
      state = 3381;
      match(TOKEN_DOT);
      state = 3382;
      match(TOKEN_K_EDGES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PeerPressureConstants_timesContext peerPressureConstants_times() {
    dynamic _localctx = PeerPressureConstants_timesContext(context, state);
    enterRule(_localctx, 466, RULE_peerPressureConstants_times);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3384;
      peerPressureStringConstant();
      state = 3385;
      match(TOKEN_DOT);
      state = 3386;
      match(TOKEN_K_TIMES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PeerPressureConstants_propertyNameContext peerPressureConstants_propertyName() {
    dynamic _localctx = PeerPressureConstants_propertyNameContext(context, state);
    enterRule(_localctx, 468, RULE_peerPressureConstants_propertyName);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3388;
      peerPressureStringConstant();
      state = 3389;
      match(TOKEN_DOT);
      state = 3390;
      match(TOKEN_K_PROPERTYNAME);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstants_targetContext shortestPathConstants_target() {
    dynamic _localctx = ShortestPathConstants_targetContext(context, state);
    enterRule(_localctx, 470, RULE_shortestPathConstants_target);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3392;
      shortestPathStringConstant();
      state = 3393;
      match(TOKEN_DOT);
      state = 3394;
      match(TOKEN_K_TARGET);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstants_edgesContext shortestPathConstants_edges() {
    dynamic _localctx = ShortestPathConstants_edgesContext(context, state);
    enterRule(_localctx, 472, RULE_shortestPathConstants_edges);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3396;
      shortestPathStringConstant();
      state = 3397;
      match(TOKEN_DOT);
      state = 3398;
      match(TOKEN_K_EDGES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstants_distanceContext shortestPathConstants_distance() {
    dynamic _localctx = ShortestPathConstants_distanceContext(context, state);
    enterRule(_localctx, 474, RULE_shortestPathConstants_distance);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3400;
      shortestPathStringConstant();
      state = 3401;
      match(TOKEN_DOT);
      state = 3402;
      match(TOKEN_K_DISTANCE);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstants_maxDistanceContext shortestPathConstants_maxDistance() {
    dynamic _localctx = ShortestPathConstants_maxDistanceContext(context, state);
    enterRule(_localctx, 476, RULE_shortestPathConstants_maxDistance);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3404;
      shortestPathStringConstant();
      state = 3405;
      match(TOKEN_DOT);
      state = 3406;
      match(TOKEN_K_MAXDISTANCE);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathConstants_includeEdgesContext shortestPathConstants_includeEdges() {
    dynamic _localctx = ShortestPathConstants_includeEdgesContext(context, state);
    enterRule(_localctx, 478, RULE_shortestPathConstants_includeEdges);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3408;
      shortestPathStringConstant();
      state = 3409;
      match(TOKEN_DOT);
      state = 3410;
      match(TOKEN_K_INCLUDEEDGES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_tokensContext withOptionsConstants_tokens() {
    dynamic _localctx = WithOptionsConstants_tokensContext(context, state);
    enterRule(_localctx, 480, RULE_withOptionsConstants_tokens);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3412;
      withOptionsStringConstant();
      state = 3413;
      match(TOKEN_DOT);
      state = 3414;
      match(TOKEN_K_TOKENS);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_noneContext withOptionsConstants_none() {
    dynamic _localctx = WithOptionsConstants_noneContext(context, state);
    enterRule(_localctx, 482, RULE_withOptionsConstants_none);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3416;
      withOptionsStringConstant();
      state = 3417;
      match(TOKEN_DOT);
      state = 3418;
      match(TOKEN_K_NONE);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_idsContext withOptionsConstants_ids() {
    dynamic _localctx = WithOptionsConstants_idsContext(context, state);
    enterRule(_localctx, 484, RULE_withOptionsConstants_ids);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3420;
      withOptionsStringConstant();
      state = 3421;
      match(TOKEN_DOT);
      state = 3422;
      match(TOKEN_K_IDS);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_labelsContext withOptionsConstants_labels() {
    dynamic _localctx = WithOptionsConstants_labelsContext(context, state);
    enterRule(_localctx, 486, RULE_withOptionsConstants_labels);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3424;
      withOptionsStringConstant();
      state = 3425;
      match(TOKEN_DOT);
      state = 3426;
      match(TOKEN_K_LABELS);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_keysContext withOptionsConstants_keys() {
    dynamic _localctx = WithOptionsConstants_keysContext(context, state);
    enterRule(_localctx, 488, RULE_withOptionsConstants_keys);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3428;
      withOptionsStringConstant();
      state = 3429;
      match(TOKEN_DOT);
      state = 3430;
      match(TOKEN_K_KEYS);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_valuesContext withOptionsConstants_values() {
    dynamic _localctx = WithOptionsConstants_valuesContext(context, state);
    enterRule(_localctx, 490, RULE_withOptionsConstants_values);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3432;
      withOptionsStringConstant();
      state = 3433;
      match(TOKEN_DOT);
      state = 3434;
      match(TOKEN_K_VALUES);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_allContext withOptionsConstants_all() {
    dynamic _localctx = WithOptionsConstants_allContext(context, state);
    enterRule(_localctx, 492, RULE_withOptionsConstants_all);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3436;
      withOptionsStringConstant();
      state = 3437;
      match(TOKEN_DOT);
      state = 3438;
      match(TOKEN_K_ALL);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_indexerContext withOptionsConstants_indexer() {
    dynamic _localctx = WithOptionsConstants_indexerContext(context, state);
    enterRule(_localctx, 494, RULE_withOptionsConstants_indexer);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3440;
      withOptionsStringConstant();
      state = 3441;
      match(TOKEN_DOT);
      state = 3442;
      match(TOKEN_K_INDEXER);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_listContext withOptionsConstants_list() {
    dynamic _localctx = WithOptionsConstants_listContext(context, state);
    enterRule(_localctx, 496, RULE_withOptionsConstants_list);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3444;
      withOptionsStringConstant();
      state = 3445;
      match(TOKEN_DOT);
      state = 3446;
      match(TOKEN_K_LIST);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsConstants_mapContext withOptionsConstants_map() {
    dynamic _localctx = WithOptionsConstants_mapContext(context, state);
    enterRule(_localctx, 498, RULE_withOptionsConstants_map);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3448;
      withOptionsStringConstant();
      state = 3449;
      match(TOKEN_DOT);
      state = 3450;
      match(TOKEN_K_MAP);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsConstants_readerContext ioOptionsConstants_reader() {
    dynamic _localctx = IoOptionsConstants_readerContext(context, state);
    enterRule(_localctx, 500, RULE_ioOptionsConstants_reader);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3452;
      ioOptionsStringConstant();
      state = 3453;
      match(TOKEN_DOT);
      state = 3454;
      match(TOKEN_K_READER);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsConstants_writerContext ioOptionsConstants_writer() {
    dynamic _localctx = IoOptionsConstants_writerContext(context, state);
    enterRule(_localctx, 502, RULE_ioOptionsConstants_writer);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3456;
      ioOptionsStringConstant();
      state = 3457;
      match(TOKEN_DOT);
      state = 3458;
      match(TOKEN_K_WRITER);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsConstants_gryoContext ioOptionsConstants_gryo() {
    dynamic _localctx = IoOptionsConstants_gryoContext(context, state);
    enterRule(_localctx, 504, RULE_ioOptionsConstants_gryo);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3460;
      ioOptionsStringConstant();
      state = 3461;
      match(TOKEN_DOT);
      state = 3462;
      match(TOKEN_K_GRYO);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsConstants_graphsonContext ioOptionsConstants_graphson() {
    dynamic _localctx = IoOptionsConstants_graphsonContext(context, state);
    enterRule(_localctx, 506, RULE_ioOptionsConstants_graphson);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3464;
      ioOptionsStringConstant();
      state = 3465;
      match(TOKEN_DOT);
      state = 3466;
      match(TOKEN_K_GRAPHSON);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsConstants_graphmlContext ioOptionsConstants_graphml() {
    dynamic _localctx = IoOptionsConstants_graphmlContext(context, state);
    enterRule(_localctx, 508, RULE_ioOptionsConstants_graphml);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3468;
      ioOptionsStringConstant();
      state = 3469;
      match(TOKEN_DOT);
      state = 3470;
      match(TOKEN_K_GRAPHML);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ConnectedComponentStringConstantContext connectedComponentStringConstant() {
    dynamic _localctx = ConnectedComponentStringConstantContext(context, state);
    enterRule(_localctx, 510, RULE_connectedComponentStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3472;
      match(TOKEN_K_CONNECTEDCOMPONENTU);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PageRankStringConstantContext pageRankStringConstant() {
    dynamic _localctx = PageRankStringConstantContext(context, state);
    enterRule(_localctx, 512, RULE_pageRankStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3474;
      match(TOKEN_K_PAGERANKU);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  PeerPressureStringConstantContext peerPressureStringConstant() {
    dynamic _localctx = PeerPressureStringConstantContext(context, state);
    enterRule(_localctx, 514, RULE_peerPressureStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3476;
      match(TOKEN_K_PEERPRESSUREU);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ShortestPathStringConstantContext shortestPathStringConstant() {
    dynamic _localctx = ShortestPathStringConstantContext(context, state);
    enterRule(_localctx, 516, RULE_shortestPathStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3478;
      match(TOKEN_K_SHORTESTPATHU);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  WithOptionsStringConstantContext withOptionsStringConstant() {
    dynamic _localctx = WithOptionsStringConstantContext(context, state);
    enterRule(_localctx, 518, RULE_withOptionsStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3480;
      match(TOKEN_K_WITHOPTOPTIONS);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IoOptionsStringConstantContext ioOptionsStringConstant() {
    dynamic _localctx = IoOptionsStringConstantContext(context, state);
    enterRule(_localctx, 520, RULE_ioOptionsStringConstant);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3482;
      match(TOKEN_K_IOU);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  BooleanArgumentContext booleanArgument() {
    dynamic _localctx = BooleanArgumentContext(context, state);
    enterRule(_localctx, 522, RULE_booleanArgument);
    try {
      state = 3486;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_FALSE:
      case TOKEN_K_TRUE:
        enterOuterAlt(_localctx, 1);
        state = 3484;
        booleanLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3485;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IntegerArgumentContext integerArgument() {
    dynamic _localctx = IntegerArgumentContext(context, state);
    enterRule(_localctx, 524, RULE_integerArgument);
    try {
      state = 3490;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_IntegerLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3488;
        integerLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3489;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringArgumentContext stringArgument() {
    dynamic _localctx = StringArgumentContext(context, state);
    enterRule(_localctx, 526, RULE_stringArgument);
    try {
      state = 3494;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_StringSuffixLiteral:
      case TOKEN_EmptyStringSuffixLiteral:
      case TOKEN_NonEmptyStringLiteral:
      case TOKEN_EmptyStringLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3492;
        stringLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3493;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringNullableArgumentContext stringNullableArgument() {
    dynamic _localctx = StringNullableArgumentContext(context, state);
    enterRule(_localctx, 528, RULE_stringNullableArgument);
    try {
      state = 3498;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_NULL:
      case TOKEN_StringSuffixLiteral:
      case TOKEN_EmptyStringSuffixLiteral:
      case TOKEN_NonEmptyStringLiteral:
      case TOKEN_EmptyStringLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3496;
        stringNullableLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3497;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringNullableArgumentVarargsContext stringNullableArgumentVarargs() {
    dynamic _localctx = StringNullableArgumentVarargsContext(context, state);
    enterRule(_localctx, 530, RULE_stringNullableArgumentVarargs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3508;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_K_NULL || ((((_la - 305)) & ~0x3f) == 0 && ((1 << (_la - 305)) & 262159) != 0)) {
        state = 3500;
        stringNullableArgument();
        state = 3505;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3501;
          match(TOKEN_COMMA);
          state = 3502;
          stringNullableArgument();
          state = 3507;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  DateArgumentContext dateArgument() {
    dynamic _localctx = DateArgumentContext(context, state);
    enterRule(_localctx, 532, RULE_dateArgument);
    try {
      state = 3512;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_DATETIME:
      case TOKEN_K_DATETIMEC:
        enterOuterAlt(_localctx, 1);
        state = 3510;
        dateLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3511;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericArgumentContext genericArgument() {
    dynamic _localctx = GenericArgumentContext(context, state);
    enterRule(_localctx, 534, RULE_genericArgument);
    try {
      state = 3516;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_ADDE:
      case TOKEN_K_ADDV:
      case TOKEN_K_AGGREGATE:
      case TOKEN_K_ALL:
      case TOKEN_K_AND:
      case TOKEN_K_ANY:
      case TOKEN_K_AS:
      case TOKEN_K_ASBOOL:
      case TOKEN_K_ASDATE:
      case TOKEN_K_ASNUMBER:
      case TOKEN_K_ASSTRING:
      case TOKEN_K_BARRIER:
      case TOKEN_K_BIGDECIMAL:
      case TOKEN_K_BIGDECIMALU:
      case TOKEN_K_BIGINT:
      case TOKEN_K_BIGINTU:
      case TOKEN_K_BINARY:
      case TOKEN_K_BINARYC:
      case TOKEN_K_BINARYU:
      case TOKEN_K_BOOLEAN:
      case TOKEN_K_BOOLEANU:
      case TOKEN_K_BOTH:
      case TOKEN_K_BOTHU:
      case TOKEN_K_BOTHE:
      case TOKEN_K_BOTHV:
      case TOKEN_K_BRANCH:
      case TOKEN_K_BY:
      case TOKEN_K_BYTE:
      case TOKEN_K_BYTEU:
      case TOKEN_K_CALL:
      case TOKEN_K_CAP:
      case TOKEN_K_CARDINALITY:
      case TOKEN_K_CHAR:
      case TOKEN_K_CHARU:
      case TOKEN_K_CHOOSE:
      case TOKEN_K_COALESCE:
      case TOKEN_K_COIN:
      case TOKEN_K_COMBINE:
      case TOKEN_K_CONCAT:
      case TOKEN_K_CONJOIN:
      case TOKEN_K_CONNECTEDCOMPONENT:
      case TOKEN_K_CONSTANT:
      case TOKEN_K_COUNT:
      case TOKEN_K_CYCLICPATH:
      case TOKEN_K_DAY:
      case TOKEN_K_DATEADD:
      case TOKEN_K_DATEDIFF:
      case TOKEN_K_DATETIME:
      case TOKEN_K_DATETIMEC:
      case TOKEN_K_DATETIMEU:
      case TOKEN_K_DEDUP:
      case TOKEN_K_DIFFERENCE:
      case TOKEN_K_DISCARD:
      case TOKEN_K_DIRECTION:
      case TOKEN_K_DISJUNCT:
      case TOKEN_K_DOUBLE:
      case TOKEN_K_DOUBLEU:
      case TOKEN_K_DROP:
      case TOKEN_K_DT:
      case TOKEN_K_DURATION:
      case TOKEN_K_DURATIONC:
      case TOKEN_K_DURATIONU:
      case TOKEN_K_E:
      case TOKEN_K_EDGE:
      case TOKEN_K_EDGEU:
      case TOKEN_K_ELEMENTMAP:
      case TOKEN_K_ELEMENT:
      case TOKEN_K_EMIT:
      case TOKEN_K_FAIL:
      case TOKEN_K_FALSE:
      case TOKEN_K_FILTER:
      case TOKEN_K_FLATMAP:
      case TOKEN_K_FLOAT:
      case TOKEN_K_FLOATU:
      case TOKEN_K_FOLD:
      case TOKEN_K_FORMAT:
      case TOKEN_K_FROM:
      case TOKEN_K_GTYPE:
      case TOKEN_K_GROUPCOUNT:
      case TOKEN_K_GROUP:
      case TOKEN_K_GRAPH:
      case TOKEN_K_GRAPHU:
      case TOKEN_K_HAS:
      case TOKEN_K_HASID:
      case TOKEN_K_HASKEY:
      case TOKEN_K_HASLABEL:
      case TOKEN_K_HASNOT:
      case TOKEN_K_HASVALUE:
      case TOKEN_K_HOUR:
      case TOKEN_K_ID:
      case TOKEN_K_IDENTITY:
      case TOKEN_K_IN:
      case TOKEN_K_INU:
      case TOKEN_K_INE:
      case TOKEN_K_INDEX:
      case TOKEN_K_INFINITY:
      case TOKEN_K_INJECT:
      case TOKEN_K_INT:
      case TOKEN_K_INTU:
      case TOKEN_K_INTERSECT:
      case TOKEN_K_INV:
      case TOKEN_K_IS:
      case TOKEN_K_KEY:
      case TOKEN_K_LABEL:
      case TOKEN_K_LENGTH:
      case TOKEN_K_LIMIT:
      case TOKEN_K_LIST:
      case TOKEN_K_LISTU:
      case TOKEN_K_LOCAL:
      case TOKEN_K_LONG:
      case TOKEN_K_LONGU:
      case TOKEN_K_LOOPS:
      case TOKEN_K_LTRIM:
      case TOKEN_K_MAP:
      case TOKEN_K_MAPU:
      case TOKEN_K_MATCH:
      case TOKEN_K_MATH:
      case TOKEN_K_MAX:
      case TOKEN_K_MEAN:
      case TOKEN_K_MERGEU:
      case TOKEN_K_MERGE:
      case TOKEN_K_MERGEE:
      case TOKEN_K_MERGEV:
      case TOKEN_K_MIN:
      case TOKEN_K_MINUTE:
      case TOKEN_K_NAN:
      case TOKEN_K_NONE:
      case TOKEN_K_NOT:
      case TOKEN_K_NULL:
      case TOKEN_K_NULLU:
      case TOKEN_K_NUMBER:
      case TOKEN_K_NUMBERU:
      case TOKEN_K_ONCREATE:
      case TOKEN_K_ONMATCH:
      case TOKEN_K_OPTION:
      case TOKEN_K_OPTIONAL:
      case TOKEN_K_ORDER:
      case TOKEN_K_OR:
      case TOKEN_K_OTHERV:
      case TOKEN_K_OUTU:
      case TOKEN_K_OUT:
      case TOKEN_K_OUTE:
      case TOKEN_K_OUTV:
      case TOKEN_K_PAGERANK:
      case TOKEN_K_PATH:
      case TOKEN_K_PATHU:
      case TOKEN_K_PEERPRESSURE:
      case TOKEN_K_PICK:
      case TOKEN_K_PROFILE:
      case TOKEN_K_PROJECT:
      case TOKEN_K_PROPERTIES:
      case TOKEN_K_PROPERTYMAP:
      case TOKEN_K_PROPERTY:
      case TOKEN_K_PROPERTYU:
      case TOKEN_K_PRODUCT:
      case TOKEN_K_RANGE:
      case TOKEN_K_READ:
      case TOKEN_K_REPLACE:
      case TOKEN_K_REPEAT:
      case TOKEN_K_REVERSE:
      case TOKEN_K_RTRIM:
      case TOKEN_K_SACK:
      case TOKEN_K_SAMPLE:
      case TOKEN_K_SECOND:
      case TOKEN_K_SELECT:
      case TOKEN_K_SET:
      case TOKEN_K_SETU:
      case TOKEN_K_SHORTESTPATH:
      case TOKEN_K_SHORT:
      case TOKEN_K_SHORTU:
      case TOKEN_K_SIDEEFFECT:
      case TOKEN_K_SIMPLEPATH:
      case TOKEN_K_SINGLE:
      case TOKEN_K_SKIP:
      case TOKEN_K_SPLIT:
      case TOKEN_K_STRING:
      case TOKEN_K_STRINGU:
      case TOKEN_K_SUBGRAPH:
      case TOKEN_K_SUBSTRING:
      case TOKEN_K_SUM:
      case TOKEN_K_T:
      case TOKEN_K_TAIL:
      case TOKEN_K_TIMELIMIT:
      case TOKEN_K_TIMES:
      case TOKEN_K_TO:
      case TOKEN_K_TOLOWER:
      case TOKEN_K_TOUPPER:
      case TOKEN_K_TOE:
      case TOKEN_K_TOV:
      case TOKEN_K_TREE:
      case TOKEN_K_TREEU:
      case TOKEN_K_TRIM:
      case TOKEN_K_TRUE:
      case TOKEN_K_UNFOLD:
      case TOKEN_K_UNION:
      case TOKEN_K_UNPRODUCTIVE:
      case TOKEN_K_UNTIL:
      case TOKEN_K_UUID:
      case TOKEN_K_UUIDL:
      case TOKEN_K_V:
      case TOKEN_K_VALUEMAP:
      case TOKEN_K_VALUES:
      case TOKEN_K_VALUE:
      case TOKEN_K_VERTEX:
      case TOKEN_K_VERTEXU:
      case TOKEN_K_VPROPERTY:
      case TOKEN_K_VPROPERTYU:
      case TOKEN_K_WHERE:
      case TOKEN_K_WITH:
      case TOKEN_K_WRITE:
      case TOKEN_IntegerLiteral:
      case TOKEN_FloatingPointLiteral:
      case TOKEN_SignedInfLiteral:
      case TOKEN_CharacterLiteral:
      case TOKEN_StringSuffixLiteral:
      case TOKEN_EmptyStringSuffixLiteral:
      case TOKEN_NonEmptyStringLiteral:
      case TOKEN_EmptyStringLiteral:
      case TOKEN_LBRACE:
      case TOKEN_LBRACK:
      case TOKEN_TRAVERSAL_ROOT:
      case TOKEN_ANON_TRAVERSAL_ROOT:
        enterOuterAlt(_localctx, 1);
        state = 3514;
        genericLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3515;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericArgumentVarargsContext genericArgumentVarargs() {
    dynamic _localctx = GenericArgumentVarargsContext(context, state);
    enterRule(_localctx, 536, RULE_genericArgumentVarargs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3526;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -4623173715914867716) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1 << (_la - 65)) & -2035911178092347441) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1 << (_la - 130)) & -5196571091927045809) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1 << (_la - 194)) & -1802571285122814369) != 0) || ((((_la - 261)) & ~0x3f) == 0 && ((1 << (_la - 261)) & 5482287297296589817) != 0)) {
        state = 3518;
        genericArgument();
        state = 3523;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3519;
          match(TOKEN_COMMA);
          state = 3520;
          genericArgument();
          state = 3525;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericMapArgumentContext genericMapArgument() {
    dynamic _localctx = GenericMapArgumentContext(context, state);
    enterRule(_localctx, 538, RULE_genericMapArgument);
    try {
      state = 3530;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_LBRACK:
        enterOuterAlt(_localctx, 1);
        state = 3528;
        genericMapLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3529;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericMapNullableArgumentContext genericMapNullableArgument() {
    dynamic _localctx = GenericMapNullableArgumentContext(context, state);
    enterRule(_localctx, 540, RULE_genericMapNullableArgument);
    try {
      state = 3534;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_K_NULL:
      case TOKEN_LBRACK:
        enterOuterAlt(_localctx, 1);
        state = 3532;
        genericMapNullableLiteral();
        break;
      case TOKEN_Identifier:
        enterOuterAlt(_localctx, 2);
        state = 3533;
        variable();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NullableGenericLiteralMapContext nullableGenericLiteralMap() {
    dynamic _localctx = NullableGenericLiteralMapContext(context, state);
    enterRule(_localctx, 542, RULE_nullableGenericLiteralMap);
    try {
      state = 3538;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_LBRACK:
        enterOuterAlt(_localctx, 1);
        state = 3536;
        genericMapLiteral();
        break;
      case TOKEN_K_NULL:
        enterOuterAlt(_localctx, 2);
        state = 3537;
        nullLiteral();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalStrategyVarargsContext traversalStrategyVarargs() {
    dynamic _localctx = TraversalStrategyVarargsContext(context, state);
    enterRule(_localctx, 544, RULE_traversalStrategyVarargs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3541;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_K_NEW || _la == TOKEN_Identifier) {
        state = 3540;
        traversalStrategyExpr();
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  TraversalStrategyExprContext traversalStrategyExpr() {
    dynamic _localctx = TraversalStrategyExprContext(context, state);
    enterRule(_localctx, 546, RULE_traversalStrategyExpr);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3543;
      traversalStrategy();
      state = 3548;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      while (_la == TOKEN_COMMA) {
        state = 3544;
        match(TOKEN_COMMA);
        state = 3545;
        traversalStrategy();
        state = 3550;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ClassTypeListContext classTypeList() {
    dynamic _localctx = ClassTypeListContext(context, state);
    enterRule(_localctx, 548, RULE_classTypeList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3552;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_Identifier) {
        state = 3551;
        classTypeExpr();
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ClassTypeExprContext classTypeExpr() {
    dynamic _localctx = ClassTypeExprContext(context, state);
    enterRule(_localctx, 550, RULE_classTypeExpr);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3554;
      classType();
      state = 3559;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      while (_la == TOKEN_COMMA) {
        state = 3555;
        match(TOKEN_COMMA);
        state = 3556;
        classType();
        state = 3561;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NestedTraversalListContext nestedTraversalList() {
    dynamic _localctx = NestedTraversalListContext(context, state);
    enterRule(_localctx, 552, RULE_nestedTraversalList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3563;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -8730458552566498308) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1 << (_la - 65)) & -6721919854052499189) != 0) || ((((_la - 132)) & ~0x3f) == 0 && ((1 << (_la - 132)) & -1441006728725095853) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1 << (_la - 197)) & 2061641772743124171) != 0) || ((((_la - 261)) & ~0x3f) == 0 && ((1 << (_la - 261)) & 576461027591938425) != 0)) {
        state = 3562;
        nestedTraversalExpr();
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NestedTraversalExprContext nestedTraversalExpr() {
    dynamic _localctx = NestedTraversalExprContext(context, state);
    enterRule(_localctx, 554, RULE_nestedTraversalExpr);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3565;
      nestedTraversal();
      state = 3570;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      while (_la == TOKEN_COMMA) {
        state = 3566;
        match(TOKEN_COMMA);
        state = 3567;
        nestedTraversal();
        state = 3572;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericCollectionLiteralContext genericCollectionLiteral() {
    dynamic _localctx = GenericCollectionLiteralContext(context, state);
    enterRule(_localctx, 556, RULE_genericCollectionLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3573;
      match(TOKEN_LBRACK);
      state = 3582;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -4623173715914867716) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1 << (_la - 65)) & -2035911178092347441) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1 << (_la - 130)) & -5196571091927045809) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1 << (_la - 194)) & -1802571285122814369) != 0) || ((((_la - 261)) & ~0x3f) == 0 && ((1 << (_la - 261)) & 870601278869201913) != 0)) {
        state = 3574;
        genericLiteral();
        state = 3579;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3575;
          match(TOKEN_COMMA);
          state = 3576;
          genericLiteral();
          state = 3581;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
      }

      state = 3584;
      match(TOKEN_RBRACK);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericLiteralVarargsContext genericLiteralVarargs() {
    dynamic _localctx = GenericLiteralVarargsContext(context, state);
    enterRule(_localctx, 558, RULE_genericLiteralVarargs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3587;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -4623173715914867716) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1 << (_la - 65)) & -2035911178092347441) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1 << (_la - 130)) & -5196571091927045809) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1 << (_la - 194)) & -1802571285122814369) != 0) || ((((_la - 261)) & ~0x3f) == 0 && ((1 << (_la - 261)) & 870601278869201913) != 0)) {
        state = 3586;
        genericLiteralExpr();
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericLiteralExprContext genericLiteralExpr() {
    dynamic _localctx = GenericLiteralExprContext(context, state);
    enterRule(_localctx, 560, RULE_genericLiteralExpr);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3589;
      genericLiteral();
      state = 3594;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      while (_la == TOKEN_COMMA) {
        state = 3590;
        match(TOKEN_COMMA);
        state = 3591;
        genericLiteral();
        state = 3596;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericMapNullableLiteralContext genericMapNullableLiteral() {
    dynamic _localctx = GenericMapNullableLiteralContext(context, state);
    enterRule(_localctx, 562, RULE_genericMapNullableLiteral);
    try {
      state = 3599;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_LBRACK:
        enterOuterAlt(_localctx, 1);
        state = 3597;
        genericMapLiteral();
        break;
      case TOKEN_K_NULL:
        enterOuterAlt(_localctx, 2);
        state = 3598;
        nullLiteral();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericRangeLiteralContext genericRangeLiteral() {
    dynamic _localctx = GenericRangeLiteralContext(context, state);
    enterRule(_localctx, 564, RULE_genericRangeLiteral);
    try {
      state = 3611;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_IntegerLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3601;
        integerLiteral();
        state = 3602;
        match(TOKEN_DOT);
        state = 3603;
        match(TOKEN_DOT);
        state = 3604;
        integerLiteral();
        break;
      case TOKEN_StringSuffixLiteral:
      case TOKEN_EmptyStringSuffixLiteral:
      case TOKEN_NonEmptyStringLiteral:
      case TOKEN_EmptyStringLiteral:
        enterOuterAlt(_localctx, 2);
        state = 3606;
        stringLiteral();
        state = 3607;
        match(TOKEN_DOT);
        state = 3608;
        match(TOKEN_DOT);
        state = 3609;
        stringLiteral();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericSetLiteralContext genericSetLiteral() {
    dynamic _localctx = GenericSetLiteralContext(context, state);
    enterRule(_localctx, 566, RULE_genericSetLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3613;
      match(TOKEN_LBRACE);
      state = 3622;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if ((((_la) & ~0x3f) == 0 && ((1 << _la) & -4623173715914867716) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1 << (_la - 65)) & -2035911178092347441) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1 << (_la - 130)) & -5196571091927045809) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1 << (_la - 194)) & -1802571285122814369) != 0) || ((((_la - 261)) & ~0x3f) == 0 && ((1 << (_la - 261)) & 870601278869201913) != 0)) {
        state = 3614;
        genericLiteral();
        state = 3619;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3615;
          match(TOKEN_COMMA);
          state = 3616;
          genericLiteral();
          state = 3621;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
      }

      state = 3624;
      match(TOKEN_RBRACE);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringNullableLiteralVarargsContext stringNullableLiteralVarargs() {
    dynamic _localctx = StringNullableLiteralVarargsContext(context, state);
    enterRule(_localctx, 568, RULE_stringNullableLiteralVarargs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3634;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_K_NULL || ((((_la - 305)) & ~0x3f) == 0 && ((1 << (_la - 305)) & 15) != 0)) {
        state = 3626;
        stringNullableLiteral();
        state = 3631;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3627;
          match(TOKEN_COMMA);
          state = 3628;
          stringNullableLiteral();
          state = 3633;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
      }

    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericLiteralContext genericLiteral() {
    dynamic _localctx = GenericLiteralContext(context, state);
    enterRule(_localctx, 570, RULE_genericLiteral);
    try {
      state = 3658;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 196, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3636;
        numericLiteral();
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3637;
        booleanLiteral();
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3638;
        stringLiteral();
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3639;
        dateLiteral();
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 3640;
        nullLiteral();
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 3641;
        traversalT();
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 3642;
        traversalCardinality();
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 3643;
        traversalDirection();
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        state = 3644;
        traversalMerge();
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        state = 3645;
        traversalPick();
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        state = 3646;
        traversalDT();
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        state = 3647;
        traversalGType();
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        state = 3648;
        genericSetLiteral();
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        state = 3649;
        genericCollectionLiteral();
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        state = 3650;
        genericRangeLiteral();
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        state = 3651;
        nestedTraversal();
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        state = 3652;
        terminatedTraversal();
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        state = 3653;
        uuidLiteral();
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        state = 3654;
        characterLiteral();
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        state = 3655;
        durationLiteral();
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        state = 3656;
        binaryLiteral();
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        state = 3657;
        genericMapLiteral();
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  GenericMapLiteralContext genericMapLiteral() {
    dynamic _localctx = GenericMapLiteralContext(context, state);
    enterRule(_localctx, 572, RULE_genericMapLiteral);
    int _la;
    try {
      state = 3674;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 198, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3660;
        match(TOKEN_LBRACK);
        state = 3661;
        match(TOKEN_COLON);
        state = 3662;
        match(TOKEN_RBRACK);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3663;
        match(TOKEN_LBRACK);
        state = 3664;
        mapEntry();
        state = 3669;
        errorHandler.sync(this);
        _la = tokenStream.LA(1)!;
        while (_la == TOKEN_COMMA) {
          state = 3665;
          match(TOKEN_COMMA);
          state = 3666;
          mapEntry();
          state = 3671;
          errorHandler.sync(this);
          _la = tokenStream.LA(1)!;
        }
        state = 3672;
        match(TOKEN_RBRACK);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  MapKeyContext mapKey() {
    dynamic _localctx = MapKeyContext(context, state);
    enterRule(_localctx, 574, RULE_mapKey);
    try {
      state = 3729;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 207, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3681;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3676;
          match(TOKEN_LPAREN);
          state = 3677;
          traversalT();
          state = 3678;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_K_T:
          state = 3680;
          traversalTLong();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3688;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3683;
          match(TOKEN_LPAREN);
          state = 3684;
          traversalDirection();
          state = 3685;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_K_DIRECTION:
          state = 3687;
          traversalDirectionLong();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3695;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3690;
          match(TOKEN_LPAREN);
          state = 3691;
          genericSetLiteral();
          state = 3692;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_LBRACE:
          state = 3694;
          genericSetLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3702;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3697;
          match(TOKEN_LPAREN);
          state = 3698;
          genericCollectionLiteral();
          state = 3699;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_LBRACK:
          state = 3701;
          genericCollectionLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        state = 3709;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3704;
          match(TOKEN_LPAREN);
          state = 3705;
          genericMapLiteral();
          state = 3706;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_LBRACK:
          state = 3708;
          genericMapLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        state = 3716;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3711;
          match(TOKEN_LPAREN);
          state = 3712;
          stringLiteral();
          state = 3713;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_StringSuffixLiteral:
        case TOKEN_EmptyStringSuffixLiteral:
        case TOKEN_NonEmptyStringLiteral:
        case TOKEN_EmptyStringLiteral:
          state = 3715;
          stringLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        state = 3723;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_LPAREN:
          state = 3718;
          match(TOKEN_LPAREN);
          state = 3719;
          numericLiteral();
          state = 3720;
          match(TOKEN_RPAREN);
          break;
        case TOKEN_K_INFINITY:
        case TOKEN_K_NAN:
        case TOKEN_IntegerLiteral:
        case TOKEN_FloatingPointLiteral:
        case TOKEN_SignedInfLiteral:
          state = 3722;
          numericLiteral();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        state = 3727;
        errorHandler.sync(this);
        switch (tokenStream.LA(1)!) {
        case TOKEN_K_ADDALL:
        case TOKEN_K_ADDE:
        case TOKEN_K_ADDV:
        case TOKEN_K_AGGREGATE:
        case TOKEN_K_ALL:
        case TOKEN_K_AND:
        case TOKEN_K_ANY:
        case TOKEN_K_AS:
        case TOKEN_K_ASBOOL:
        case TOKEN_K_ASC:
        case TOKEN_K_ASDATE:
        case TOKEN_K_ASNUMBER:
        case TOKEN_K_ASSTRING:
        case TOKEN_K_ASSIGN:
        case TOKEN_K_BARRIER:
        case TOKEN_K_BARRIERU:
        case TOKEN_K_BEGIN:
        case TOKEN_K_BETWEEN:
        case TOKEN_K_BIGDECIMAL:
        case TOKEN_K_BIGDECIMALU:
        case TOKEN_K_BIGINT:
        case TOKEN_K_BIGINTU:
        case TOKEN_K_BINARY:
        case TOKEN_K_BINARYC:
        case TOKEN_K_BINARYU:
        case TOKEN_K_BOOLEAN:
        case TOKEN_K_BOOLEANU:
        case TOKEN_K_BOTH:
        case TOKEN_K_BOTHU:
        case TOKEN_K_BOTHE:
        case TOKEN_K_BOTHV:
        case TOKEN_K_BRANCH:
        case TOKEN_K_BY:
        case TOKEN_K_BYTE:
        case TOKEN_K_BYTEU:
        case TOKEN_K_CALL:
        case TOKEN_K_CAP:
        case TOKEN_K_CARDINALITY:
        case TOKEN_K_CHAR:
        case TOKEN_K_CHARU:
        case TOKEN_K_CHOOSE:
        case TOKEN_K_COALESCE:
        case TOKEN_K_COIN:
        case TOKEN_K_COLUMN:
        case TOKEN_K_COMBINE:
        case TOKEN_K_COMMIT:
        case TOKEN_K_COMPONENT:
        case TOKEN_K_CONCAT:
        case TOKEN_K_CONJOIN:
        case TOKEN_K_CONNECTEDCOMPONENT:
        case TOKEN_K_CONNECTEDCOMPONENTU:
        case TOKEN_K_CONSTANT:
        case TOKEN_K_CONTAINING:
        case TOKEN_K_COUNT:
        case TOKEN_K_CYCLICPATH:
        case TOKEN_K_DAY:
        case TOKEN_K_DATEADD:
        case TOKEN_K_DATEDIFF:
        case TOKEN_K_DATETIME:
        case TOKEN_K_DATETIMEC:
        case TOKEN_K_DATETIMEU:
        case TOKEN_K_DECR:
        case TOKEN_K_DEDUP:
        case TOKEN_K_DESC:
        case TOKEN_K_DIFFERENCE:
        case TOKEN_K_DISCARD:
        case TOKEN_K_DIRECTION:
        case TOKEN_K_DISJUNCT:
        case TOKEN_K_DISTANCE:
        case TOKEN_K_DIV:
        case TOKEN_K_DOUBLE:
        case TOKEN_K_DOUBLEU:
        case TOKEN_K_DROP:
        case TOKEN_K_DT:
        case TOKEN_K_DURATION:
        case TOKEN_K_DURATIONC:
        case TOKEN_K_DURATIONU:
        case TOKEN_K_E:
        case TOKEN_K_EDGE:
        case TOKEN_K_EDGEU:
        case TOKEN_K_EDGES:
        case TOKEN_K_ELEMENTMAP:
        case TOKEN_K_ELEMENT:
        case TOKEN_K_EMIT:
        case TOKEN_K_ENDINGWITH:
        case TOKEN_K_EQ:
        case TOKEN_K_EXPLAIN:
        case TOKEN_K_FAIL:
        case TOKEN_K_FALSE:
        case TOKEN_K_FILTER:
        case TOKEN_K_FIRST:
        case TOKEN_K_FLATMAP:
        case TOKEN_K_FLOAT:
        case TOKEN_K_FLOATU:
        case TOKEN_K_FOLD:
        case TOKEN_K_FORMAT:
        case TOKEN_K_FROM:
        case TOKEN_K_GLOBAL:
        case TOKEN_K_GT:
        case TOKEN_K_GTE:
        case TOKEN_K_GTYPE:
        case TOKEN_K_GRAPHML:
        case TOKEN_K_GRAPHSON:
        case TOKEN_K_GROUPCOUNT:
        case TOKEN_K_GROUP:
        case TOKEN_K_GRYO:
        case TOKEN_K_GRAPH:
        case TOKEN_K_GRAPHU:
        case TOKEN_K_HAS:
        case TOKEN_K_HASID:
        case TOKEN_K_HASKEY:
        case TOKEN_K_HASLABEL:
        case TOKEN_K_HASNEXT:
        case TOKEN_K_HASNOT:
        case TOKEN_K_HASVALUE:
        case TOKEN_K_HOUR:
        case TOKEN_K_ID:
        case TOKEN_K_IDENTITY:
        case TOKEN_K_IDS:
        case TOKEN_K_IN:
        case TOKEN_K_INU:
        case TOKEN_K_INE:
        case TOKEN_K_INCLUDEEDGES:
        case TOKEN_K_INCR:
        case TOKEN_K_INDEXER:
        case TOKEN_K_INDEX:
        case TOKEN_K_INFINITY:
        case TOKEN_K_INJECT:
        case TOKEN_K_INSIDE:
        case TOKEN_K_INT:
        case TOKEN_K_INTU:
        case TOKEN_K_INTERSECT:
        case TOKEN_K_INV:
        case TOKEN_K_IOU:
        case TOKEN_K_IO:
        case TOKEN_K_IS:
        case TOKEN_K_ITERATE:
        case TOKEN_K_KEY:
        case TOKEN_K_KEYS:
        case TOKEN_K_LABELS:
        case TOKEN_K_LABEL:
        case TOKEN_K_LAST:
        case TOKEN_K_LENGTH:
        case TOKEN_K_LIMIT:
        case TOKEN_K_LIST:
        case TOKEN_K_LISTU:
        case TOKEN_K_LOCAL:
        case TOKEN_K_LONG:
        case TOKEN_K_LONGU:
        case TOKEN_K_LOOPS:
        case TOKEN_K_LT:
        case TOKEN_K_LTE:
        case TOKEN_K_LTRIM:
        case TOKEN_K_MAP:
        case TOKEN_K_MAPU:
        case TOKEN_K_MATCH:
        case TOKEN_K_MATH:
        case TOKEN_K_MAX:
        case TOKEN_K_MAXDISTANCE:
        case TOKEN_K_MEAN:
        case TOKEN_K_MERGEU:
        case TOKEN_K_MERGE:
        case TOKEN_K_MERGEE:
        case TOKEN_K_MERGEV:
        case TOKEN_K_MIN:
        case TOKEN_K_MINUTE:
        case TOKEN_K_MINUS:
        case TOKEN_K_MIXED:
        case TOKEN_K_MULT:
        case TOKEN_K_N:
        case TOKEN_K_NAN:
        case TOKEN_K_NEGATE:
        case TOKEN_K_NEXT:
        case TOKEN_K_NONE:
        case TOKEN_K_NOTREGEX:
        case TOKEN_K_NOTCONTAINING:
        case TOKEN_K_NOTENDINGWITH:
        case TOKEN_K_NOTSTARTINGWITH:
        case TOKEN_K_NOT:
        case TOKEN_K_NEQ:
        case TOKEN_K_NEW:
        case TOKEN_K_NORMSACK:
        case TOKEN_K_NULL:
        case TOKEN_K_NULLU:
        case TOKEN_K_NUMBER:
        case TOKEN_K_NUMBERU:
        case TOKEN_K_ONCREATE:
        case TOKEN_K_ONMATCH:
        case TOKEN_K_OPERATOR:
        case TOKEN_K_OPTION:
        case TOKEN_K_OPTIONAL:
        case TOKEN_K_ORDERU:
        case TOKEN_K_ORDER:
        case TOKEN_K_OR:
        case TOKEN_K_OTHERV:
        case TOKEN_K_OUTU:
        case TOKEN_K_OUT:
        case TOKEN_K_OUTE:
        case TOKEN_K_OUTSIDE:
        case TOKEN_K_OUTV:
        case TOKEN_K_P:
        case TOKEN_K_PAGERANKU:
        case TOKEN_K_PAGERANK:
        case TOKEN_K_PATH:
        case TOKEN_K_PATHU:
        case TOKEN_K_PEERPRESSUREU:
        case TOKEN_K_PEERPRESSURE:
        case TOKEN_K_PICK:
        case TOKEN_K_POP:
        case TOKEN_K_PROFILE:
        case TOKEN_K_PROJECT:
        case TOKEN_K_PROPERTIES:
        case TOKEN_K_PROPERTYMAP:
        case TOKEN_K_PROPERTYNAME:
        case TOKEN_K_PROPERTY:
        case TOKEN_K_PROPERTYU:
        case TOKEN_K_PRODUCT:
        case TOKEN_K_RANGE:
        case TOKEN_K_READ:
        case TOKEN_K_READER:
        case TOKEN_K_REGEX:
        case TOKEN_K_REPLACE:
        case TOKEN_K_REPEAT:
        case TOKEN_K_REVERSE:
        case TOKEN_K_ROLLBACK:
        case TOKEN_K_RTRIM:
        case TOKEN_K_SACK:
        case TOKEN_K_SAMPLE:
        case TOKEN_K_SCOPE:
        case TOKEN_K_SECOND:
        case TOKEN_K_SELECT:
        case TOKEN_K_SET:
        case TOKEN_K_SETU:
        case TOKEN_K_SHORTESTPATHU:
        case TOKEN_K_SHORTESTPATH:
        case TOKEN_K_SHUFFLE:
        case TOKEN_K_SHORT:
        case TOKEN_K_SHORTU:
        case TOKEN_K_SIDEEFFECT:
        case TOKEN_K_SIMPLEPATH:
        case TOKEN_K_SINGLE:
        case TOKEN_K_SKIP:
        case TOKEN_K_SPLIT:
        case TOKEN_K_STARTINGWITH:
        case TOKEN_K_STRING:
        case TOKEN_K_STRINGU:
        case TOKEN_K_SUBGRAPH:
        case TOKEN_K_SUBSTRING:
        case TOKEN_K_SUM:
        case TOKEN_K_SUMLONG:
        case TOKEN_K_T:
        case TOKEN_K_TAIL:
        case TOKEN_K_TARGET:
        case TOKEN_K_TEXTP:
        case TOKEN_K_TIMELIMIT:
        case TOKEN_K_TIMES:
        case TOKEN_K_TO:
        case TOKEN_K_TOBULKSET:
        case TOKEN_K_TOKENS:
        case TOKEN_K_TOLIST:
        case TOKEN_K_TOLOWER:
        case TOKEN_K_TOSET:
        case TOKEN_K_TOSTRING:
        case TOKEN_K_TOUPPER:
        case TOKEN_K_TOE:
        case TOKEN_K_TOV:
        case TOKEN_K_TREE:
        case TOKEN_K_TREEU:
        case TOKEN_K_TRIM:
        case TOKEN_K_TRUE:
        case TOKEN_K_TRYNEXT:
        case TOKEN_K_TYPEOF:
        case TOKEN_K_TX:
        case TOKEN_K_UNFOLD:
        case TOKEN_K_UNION:
        case TOKEN_K_UNPRODUCTIVE:
        case TOKEN_K_UNTIL:
        case TOKEN_K_UUID:
        case TOKEN_K_UUIDL:
        case TOKEN_K_V:
        case TOKEN_K_VALUEMAP:
        case TOKEN_K_VALUES:
        case TOKEN_K_VALUE:
        case TOKEN_K_VERTEX:
        case TOKEN_K_VERTEXU:
        case TOKEN_K_VPROPERTY:
        case TOKEN_K_VPROPERTYU:
        case TOKEN_K_WHERE:
        case TOKEN_K_WITH:
        case TOKEN_K_WITHBULK:
        case TOKEN_K_WITHIN:
        case TOKEN_K_WITHOPTOPTIONS:
        case TOKEN_K_WITHOUT:
        case TOKEN_K_WITHOUTSTRATEGIES:
        case TOKEN_K_WITHPATH:
        case TOKEN_K_WITHSACK:
        case TOKEN_K_WITHSIDEEFFECT:
        case TOKEN_K_WITHSTRATEGIES:
        case TOKEN_K_WRITE:
        case TOKEN_K_WRITER:
        case TOKEN_TRAVERSAL_ROOT:
          state = 3725;
          keyword();
          break;
        case TOKEN_Identifier:
          state = 3726;
          nakedKey();
          break;
        default:
          throw NoViableAltException(this);
        }
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  MapEntryContext mapEntry() {
    dynamic _localctx = MapEntryContext(context, state);
    enterRule(_localctx, 576, RULE_mapEntry);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3731;
      mapKey();
      state = 3732;
      match(TOKEN_COLON);
      state = 3733;
      genericLiteral();
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringLiteralContext stringLiteral() {
    dynamic _localctx = StringLiteralContext(context, state);
    enterRule(_localctx, 578, RULE_stringLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3735;
      _la = tokenStream.LA(1)!;
      if (!(((((_la - 305)) & ~0x3f) == 0 && ((1 << (_la - 305)) & 15) != 0))) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  StringNullableLiteralContext stringNullableLiteral() {
    dynamic _localctx = StringNullableLiteralContext(context, state);
    enterRule(_localctx, 580, RULE_stringNullableLiteral);
    try {
      state = 3739;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_StringSuffixLiteral:
      case TOKEN_EmptyStringSuffixLiteral:
      case TOKEN_NonEmptyStringLiteral:
      case TOKEN_EmptyStringLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3737;
        stringLiteral();
        break;
      case TOKEN_K_NULL:
        enterOuterAlt(_localctx, 2);
        state = 3738;
        match(TOKEN_K_NULL);
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  IntegerLiteralContext integerLiteral() {
    dynamic _localctx = IntegerLiteralContext(context, state);
    enterRule(_localctx, 582, RULE_integerLiteral);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3741;
      match(TOKEN_IntegerLiteral);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  FloatLiteralContext floatLiteral() {
    dynamic _localctx = FloatLiteralContext(context, state);
    enterRule(_localctx, 584, RULE_floatLiteral);
    try {
      state = 3746;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_FloatingPointLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3743;
        match(TOKEN_FloatingPointLiteral);
        break;
      case TOKEN_K_INFINITY:
      case TOKEN_SignedInfLiteral:
        enterOuterAlt(_localctx, 2);
        state = 3744;
        infLiteral();
        break;
      case TOKEN_K_NAN:
        enterOuterAlt(_localctx, 3);
        state = 3745;
        nanLiteral();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NumericLiteralContext numericLiteral() {
    dynamic _localctx = NumericLiteralContext(context, state);
    enterRule(_localctx, 586, RULE_numericLiteral);
    try {
      state = 3750;
      errorHandler.sync(this);
      switch (tokenStream.LA(1)!) {
      case TOKEN_IntegerLiteral:
        enterOuterAlt(_localctx, 1);
        state = 3748;
        integerLiteral();
        break;
      case TOKEN_K_INFINITY:
      case TOKEN_K_NAN:
      case TOKEN_FloatingPointLiteral:
      case TOKEN_SignedInfLiteral:
        enterOuterAlt(_localctx, 2);
        state = 3749;
        floatLiteral();
        break;
      default:
        throw NoViableAltException(this);
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  BooleanLiteralContext booleanLiteral() {
    dynamic _localctx = BooleanLiteralContext(context, state);
    enterRule(_localctx, 588, RULE_booleanLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3752;
      _la = tokenStream.LA(1)!;
      if (!(_la == TOKEN_K_FALSE || _la == TOKEN_K_TRUE)) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  DateLiteralContext dateLiteral() {
    dynamic _localctx = DateLiteralContext(context, state);
    enterRule(_localctx, 590, RULE_dateLiteral);
    try {
      state = 3770;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 211, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3754;
        match(TOKEN_K_DATETIME);
        state = 3755;
        match(TOKEN_LPAREN);
        state = 3756;
        stringLiteral();
        state = 3757;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3759;
        match(TOKEN_K_DATETIME);
        state = 3760;
        match(TOKEN_LPAREN);
        state = 3761;
        match(TOKEN_RPAREN);
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        state = 3762;
        match(TOKEN_K_DATETIMEC);
        state = 3763;
        match(TOKEN_LPAREN);
        state = 3764;
        stringLiteral();
        state = 3765;
        match(TOKEN_RPAREN);
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        state = 3767;
        match(TOKEN_K_DATETIMEC);
        state = 3768;
        match(TOKEN_LPAREN);
        state = 3769;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NullLiteralContext nullLiteral() {
    dynamic _localctx = NullLiteralContext(context, state);
    enterRule(_localctx, 592, RULE_nullLiteral);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3772;
      match(TOKEN_K_NULL);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NanLiteralContext nanLiteral() {
    dynamic _localctx = NanLiteralContext(context, state);
    enterRule(_localctx, 594, RULE_nanLiteral);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3774;
      match(TOKEN_K_NAN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  InfLiteralContext infLiteral() {
    dynamic _localctx = InfLiteralContext(context, state);
    enterRule(_localctx, 596, RULE_infLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3776;
      _la = tokenStream.LA(1)!;
      if (!(_la == TOKEN_K_INFINITY || _la == TOKEN_SignedInfLiteral)) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  UuidLiteralContext uuidLiteral() {
    dynamic _localctx = UuidLiteralContext(context, state);
    enterRule(_localctx, 598, RULE_uuidLiteral);
    try {
      state = 3786;
      errorHandler.sync(this);
      switch (interpreter!.adaptivePredict(tokenStream, 212, context)) {
      case 1:
        enterOuterAlt(_localctx, 1);
        state = 3778;
        match(TOKEN_K_UUID);
        state = 3779;
        match(TOKEN_LPAREN);
        state = 3780;
        match(TOKEN_RPAREN);
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        state = 3781;
        match(TOKEN_K_UUID);
        state = 3782;
        match(TOKEN_LPAREN);
        state = 3783;
        stringLiteral();
        state = 3784;
        match(TOKEN_RPAREN);
        break;
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  CharacterLiteralContext characterLiteral() {
    dynamic _localctx = CharacterLiteralContext(context, state);
    enterRule(_localctx, 600, RULE_characterLiteral);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3788;
      match(TOKEN_CharacterLiteral);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  DurationLiteralContext durationLiteral() {
    dynamic _localctx = DurationLiteralContext(context, state);
    enterRule(_localctx, 602, RULE_durationLiteral);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3790;
      match(TOKEN_K_DURATIONC);
      state = 3791;
      match(TOKEN_LPAREN);
      state = 3792;
      integerLiteral();
      state = 3793;
      match(TOKEN_COMMA);
      state = 3794;
      integerLiteral();
      state = 3797;
      errorHandler.sync(this);
      _la = tokenStream.LA(1)!;
      if (_la == TOKEN_COMMA) {
        state = 3795;
        match(TOKEN_COMMA);
        state = 3796;
        booleanLiteral();
      }

      state = 3799;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  BinaryLiteralContext binaryLiteral() {
    dynamic _localctx = BinaryLiteralContext(context, state);
    enterRule(_localctx, 604, RULE_binaryLiteral);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3801;
      match(TOKEN_K_BINARYC);
      state = 3802;
      match(TOKEN_LPAREN);
      state = 3803;
      stringLiteral();
      state = 3804;
      match(TOKEN_RPAREN);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  NakedKeyContext nakedKey() {
    dynamic _localctx = NakedKeyContext(context, state);
    enterRule(_localctx, 606, RULE_nakedKey);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3806;
      match(TOKEN_Identifier);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  ClassTypeContext classType() {
    dynamic _localctx = ClassTypeContext(context, state);
    enterRule(_localctx, 608, RULE_classType);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3808;
      match(TOKEN_Identifier);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  VariableContext variable() {
    dynamic _localctx = VariableContext(context, state);
    enterRule(_localctx, 610, RULE_variable);
    try {
      enterOuterAlt(_localctx, 1);
      state = 3810;
      match(TOKEN_Identifier);
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  KeywordContext keyword() {
    dynamic _localctx = KeywordContext(context, state);
    enterRule(_localctx, 612, RULE_keyword);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      state = 3812;
      _la = tokenStream.LA(1)!;
      if (!((((_la) & ~0x3f) == 0 && ((1 << _la) & -2) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1 << (_la - 64)) & -1) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1 << (_la - 128)) & -1) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1 << (_la - 192)) & -1) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1 << (_la - 256)) & -9223336852482686977) != 0))) {
      errorHandler.recoverInline(this);
      } else {
        if ( tokenStream.LA(1)! == IntStream.EOF ) matchedEOF = true;
        errorHandler.reportMatch(this);
        consume();
      }
    } on RecognitionException catch (re) {
      _localctx.exception = re;
      errorHandler.reportError(this, re);
      errorHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  @override
  bool sempred(RuleContext? _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return _query_sempred(_localctx as QueryContext?, predIndex);
    case 3:
      return _traversalSource_sempred(_localctx as TraversalSourceContext?, predIndex);
    case 25:
      return _chainedTraversal_sempred(_localctx as ChainedTraversalContext?, predIndex);
    case 183:
      return _traversalPredicate_sempred(_localctx as TraversalPredicateContext?, predIndex);
    }
    return true;
  }
  bool _query_sempred(dynamic _localctx, int predIndex) {
    switch (predIndex) {
      case 0: return precpred(context, 2);
    }
    return true;
  }
  bool _traversalSource_sempred(dynamic _localctx, int predIndex) {
    switch (predIndex) {
      case 1: return precpred(context, 1);
    }
    return true;
  }
  bool _chainedTraversal_sempred(dynamic _localctx, int predIndex) {
    switch (predIndex) {
      case 2: return precpred(context, 1);
    }
    return true;
  }
  bool _traversalPredicate_sempred(dynamic _localctx, int predIndex) {
    switch (predIndex) {
      case 3: return precpred(context, 3);
      case 4: return precpred(context, 2);
      case 5: return precpred(context, 1);
    }
    return true;
  }

  static const List<int> _serializedATN = [
      4,1,323,3815,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
      6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,
      2,14,7,14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,
      20,2,21,7,21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,
      7,27,2,28,7,28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,
      34,7,34,2,35,7,35,2,36,7,36,2,37,7,37,2,38,7,38,2,39,7,39,2,40,7,40,
      2,41,7,41,2,42,7,42,2,43,7,43,2,44,7,44,2,45,7,45,2,46,7,46,2,47,7,
      47,2,48,7,48,2,49,7,49,2,50,7,50,2,51,7,51,2,52,7,52,2,53,7,53,2,54,
      7,54,2,55,7,55,2,56,7,56,2,57,7,57,2,58,7,58,2,59,7,59,2,60,7,60,2,
      61,7,61,2,62,7,62,2,63,7,63,2,64,7,64,2,65,7,65,2,66,7,66,2,67,7,67,
      2,68,7,68,2,69,7,69,2,70,7,70,2,71,7,71,2,72,7,72,2,73,7,73,2,74,7,
      74,2,75,7,75,2,76,7,76,2,77,7,77,2,78,7,78,2,79,7,79,2,80,7,80,2,81,
      7,81,2,82,7,82,2,83,7,83,2,84,7,84,2,85,7,85,2,86,7,86,2,87,7,87,2,
      88,7,88,2,89,7,89,2,90,7,90,2,91,7,91,2,92,7,92,2,93,7,93,2,94,7,94,
      2,95,7,95,2,96,7,96,2,97,7,97,2,98,7,98,2,99,7,99,2,100,7,100,2,101,
      7,101,2,102,7,102,2,103,7,103,2,104,7,104,2,105,7,105,2,106,7,106,
      2,107,7,107,2,108,7,108,2,109,7,109,2,110,7,110,2,111,7,111,2,112,
      7,112,2,113,7,113,2,114,7,114,2,115,7,115,2,116,7,116,2,117,7,117,
      2,118,7,118,2,119,7,119,2,120,7,120,2,121,7,121,2,122,7,122,2,123,
      7,123,2,124,7,124,2,125,7,125,2,126,7,126,2,127,7,127,2,128,7,128,
      2,129,7,129,2,130,7,130,2,131,7,131,2,132,7,132,2,133,7,133,2,134,
      7,134,2,135,7,135,2,136,7,136,2,137,7,137,2,138,7,138,2,139,7,139,
      2,140,7,140,2,141,7,141,2,142,7,142,2,143,7,143,2,144,7,144,2,145,
      7,145,2,146,7,146,2,147,7,147,2,148,7,148,2,149,7,149,2,150,7,150,
      2,151,7,151,2,152,7,152,2,153,7,153,2,154,7,154,2,155,7,155,2,156,
      7,156,2,157,7,157,2,158,7,158,2,159,7,159,2,160,7,160,2,161,7,161,
      2,162,7,162,2,163,7,163,2,164,7,164,2,165,7,165,2,166,7,166,2,167,
      7,167,2,168,7,168,2,169,7,169,2,170,7,170,2,171,7,171,2,172,7,172,
      2,173,7,173,2,174,7,174,2,175,7,175,2,176,7,176,2,177,7,177,2,178,
      7,178,2,179,7,179,2,180,7,180,2,181,7,181,2,182,7,182,2,183,7,183,
      2,184,7,184,2,185,7,185,2,186,7,186,2,187,7,187,2,188,7,188,2,189,
      7,189,2,190,7,190,2,191,7,191,2,192,7,192,2,193,7,193,2,194,7,194,
      2,195,7,195,2,196,7,196,2,197,7,197,2,198,7,198,2,199,7,199,2,200,
      7,200,2,201,7,201,2,202,7,202,2,203,7,203,2,204,7,204,2,205,7,205,
      2,206,7,206,2,207,7,207,2,208,7,208,2,209,7,209,2,210,7,210,2,211,
      7,211,2,212,7,212,2,213,7,213,2,214,7,214,2,215,7,215,2,216,7,216,
      2,217,7,217,2,218,7,218,2,219,7,219,2,220,7,220,2,221,7,221,2,222,
      7,222,2,223,7,223,2,224,7,224,2,225,7,225,2,226,7,226,2,227,7,227,
      2,228,7,228,2,229,7,229,2,230,7,230,2,231,7,231,2,232,7,232,2,233,
      7,233,2,234,7,234,2,235,7,235,2,236,7,236,2,237,7,237,2,238,7,238,
      2,239,7,239,2,240,7,240,2,241,7,241,2,242,7,242,2,243,7,243,2,244,
      7,244,2,245,7,245,2,246,7,246,2,247,7,247,2,248,7,248,2,249,7,249,
      2,250,7,250,2,251,7,251,2,252,7,252,2,253,7,253,2,254,7,254,2,255,
      7,255,2,256,7,256,2,257,7,257,2,258,7,258,2,259,7,259,2,260,7,260,
      2,261,7,261,2,262,7,262,2,263,7,263,2,264,7,264,2,265,7,265,2,266,
      7,266,2,267,7,267,2,268,7,268,2,269,7,269,2,270,7,270,2,271,7,271,
      2,272,7,272,2,273,7,273,2,274,7,274,2,275,7,275,2,276,7,276,2,277,
      7,277,2,278,7,278,2,279,7,279,2,280,7,280,2,281,7,281,2,282,7,282,
      2,283,7,283,2,284,7,284,2,285,7,285,2,286,7,286,2,287,7,287,2,288,
      7,288,2,289,7,289,2,290,7,290,2,291,7,291,2,292,7,292,2,293,7,293,
      2,294,7,294,2,295,7,295,2,296,7,296,2,297,7,297,2,298,7,298,2,299,
      7,299,2,300,7,300,2,301,7,301,2,302,7,302,2,303,7,303,2,304,7,304,
      2,305,7,305,2,306,7,306,1,0,1,0,3,0,617,8,0,1,0,5,0,620,8,0,10,0,12,
      0,623,9,0,1,0,3,0,626,8,0,1,0,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
      1,1,1,1,1,1,1,1,3,1,642,8,1,1,1,1,1,1,1,1,1,1,1,5,1,649,8,1,10,1,12,
      1,652,9,1,1,2,1,2,1,3,1,3,1,3,1,3,1,3,3,3,661,8,3,1,3,1,3,1,3,5,3,
      666,8,3,10,3,12,3,669,9,3,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,
      1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,4,3,4,692,8,4,1,5,1,5,1,
      5,1,5,1,5,1,5,1,5,1,5,1,5,1,5,3,5,704,8,5,1,6,1,6,1,6,1,6,1,6,1,6,
      1,6,3,6,713,8,6,1,7,1,7,1,7,1,7,1,7,1,8,1,8,1,8,1,8,1,9,1,9,1,9,1,
      9,1,9,1,9,1,9,1,9,1,9,1,9,1,9,1,9,3,9,736,8,9,1,10,1,10,1,10,1,10,
      1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,10,3,10,754,
      8,10,1,11,1,11,1,11,1,11,1,11,3,11,761,8,11,1,11,1,11,1,12,1,12,1,
      12,1,12,1,12,3,12,770,8,12,1,12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,
      1,13,1,13,1,13,1,13,1,13,1,13,3,13,786,8,13,1,14,1,14,1,14,1,14,1,
      14,1,14,1,14,1,14,1,14,1,14,3,14,798,8,14,1,15,1,15,1,15,1,15,1,15,
      1,15,1,15,1,15,1,15,1,15,3,15,810,8,15,1,16,1,16,1,16,1,16,1,16,1,
      16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,3,16,825,8,16,1,17,1,17,1,17,
      1,17,1,17,1,18,1,18,1,18,1,18,1,18,1,19,1,19,1,19,1,19,1,19,1,20,1,
      20,1,20,1,20,1,20,1,21,1,21,1,21,1,21,1,21,1,21,1,21,1,21,1,21,1,21,
      3,21,857,8,21,1,22,1,22,1,22,1,22,1,22,1,22,1,22,1,22,1,22,1,22,3,
      22,869,8,22,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,
      1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,23,1,
      23,1,23,1,23,1,23,1,23,1,23,1,23,3,23,902,8,23,1,24,1,24,1,24,1,24,
      1,24,1,25,1,25,1,25,1,25,1,25,1,25,5,25,915,8,25,10,25,12,25,918,9,
      25,1,26,1,26,1,26,1,26,3,26,924,8,26,1,27,1,27,1,27,1,27,1,28,1,28,
      1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,
      28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,
      1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,
      28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,
      1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,
      28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,
      1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,
      28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,
      1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,
      28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,1,28,3,28,1065,
      8,28,1,29,1,29,1,29,1,29,1,29,1,30,1,30,1,30,1,30,1,30,1,31,1,31,1,
      31,1,31,1,31,1,31,1,31,1,31,1,31,1,31,3,31,1087,8,31,1,32,1,32,1,32,
      1,32,1,32,1,32,1,32,1,32,1,32,1,32,1,32,1,32,1,32,3,32,1102,8,32,1,
      33,1,33,1,33,1,33,1,33,1,34,1,34,1,34,1,34,1,34,1,35,1,35,1,35,1,35,
      1,35,1,36,1,36,1,36,1,36,1,36,1,37,1,37,1,37,1,37,1,37,3,37,1129,8,
      37,1,37,1,37,1,38,1,38,1,38,1,38,1,39,1,39,1,39,1,39,1,40,1,40,1,40,
      1,40,1,40,1,40,1,40,1,40,3,40,1149,8,40,1,41,1,41,1,41,1,41,1,41,1,
      41,1,41,1,41,3,41,1159,8,41,1,42,1,42,1,42,1,42,1,42,1,42,1,42,1,42,
      1,42,1,42,1,42,1,42,1,42,3,42,1174,8,42,1,43,1,43,1,43,1,43,1,43,1,
      44,1,44,1,44,1,44,1,44,1,45,1,45,1,45,1,45,1,46,1,46,1,46,1,46,1,46,
      1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,
      47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,
      1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,
      47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,1,47,
      3,47,1249,8,47,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,
      48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,1,48,
      1,48,1,48,1,48,1,48,3,48,1279,8,48,1,49,1,49,1,49,1,49,1,49,3,49,1286,
      8,49,1,49,1,49,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,
      50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,
      1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,50,1,
      50,1,50,1,50,1,50,1,50,3,50,1332,8,50,1,51,1,51,1,51,1,51,1,51,1,52,
      1,52,1,52,1,52,1,52,1,53,1,53,1,53,1,53,1,53,1,54,1,54,1,54,1,54,1,
      54,3,54,1354,8,54,1,54,1,54,1,54,1,54,1,54,1,54,1,54,3,54,1363,8,54,
      1,55,1,55,1,55,1,55,1,55,1,56,1,56,1,56,1,56,1,57,1,57,1,57,1,57,1,
      57,1,58,1,58,1,58,1,58,1,58,1,58,1,58,1,58,3,58,1387,8,58,1,59,1,59,
      1,59,1,59,1,60,1,60,1,60,1,60,1,60,1,60,1,60,1,61,1,61,1,61,1,61,1,
      61,1,61,1,61,1,61,1,61,1,61,3,61,1410,8,61,1,62,1,62,1,62,1,62,1,62,
      3,62,1417,8,62,1,62,1,62,1,62,1,62,1,62,1,62,1,62,3,62,1426,8,62,1,
      63,1,63,1,63,1,63,1,63,1,64,1,64,1,64,1,64,1,65,1,65,1,65,1,65,1,65,
      1,66,1,66,1,66,1,66,1,67,1,67,1,67,1,67,1,68,1,68,1,68,1,68,1,68,1,
      69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,1,69,3,69,
      1468,8,69,1,70,1,70,1,70,1,70,1,70,1,70,1,70,1,70,3,70,1478,8,70,1,
      71,1,71,1,71,1,71,1,71,1,71,1,71,1,71,1,71,1,71,3,71,1490,8,71,1,72,
      1,72,1,72,1,72,1,72,1,73,1,73,1,73,1,73,1,73,1,73,1,73,1,73,1,73,1,
      73,3,73,1507,8,73,1,74,1,74,1,74,1,74,1,74,1,75,1,75,1,75,1,75,1,75,
      1,75,1,75,1,75,1,75,1,75,3,75,1524,8,75,1,76,1,76,1,76,1,76,1,76,1,
      76,1,76,1,76,3,76,1534,8,76,1,77,1,77,1,77,1,77,1,77,1,77,1,77,1,77,
      3,77,1544,8,77,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,
      78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,
      1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,
      78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,1,78,
      3,78,1597,8,78,1,79,1,79,1,79,1,79,1,79,3,79,1604,8,79,1,79,1,79,1,
      79,1,79,1,79,1,79,1,79,3,79,1613,8,79,1,80,1,80,1,80,1,80,1,80,1,80,
      1,80,1,80,1,80,1,80,3,80,1625,8,80,1,80,1,80,3,80,1629,8,80,1,81,1,
      81,1,81,1,81,1,81,1,81,1,81,1,81,1,81,1,81,3,81,1641,8,81,1,81,1,81,
      3,81,1645,8,81,1,82,1,82,1,82,1,82,1,82,1,83,1,83,1,83,1,83,1,83,3,
      83,1657,8,83,1,83,1,83,1,83,1,83,1,83,1,83,1,83,3,83,1666,8,83,1,84,
      1,84,1,84,1,84,1,85,1,85,1,85,1,85,1,86,1,86,1,86,1,86,1,86,1,87,1,
      87,1,87,1,87,1,87,1,88,1,88,1,88,1,88,1,88,1,89,1,89,1,89,1,89,1,90,
      1,90,1,90,1,90,1,91,1,91,1,91,1,91,1,91,1,92,1,92,1,92,1,92,1,92,1,
      92,1,92,1,92,1,92,1,92,3,92,1714,8,92,1,93,1,93,1,93,1,93,1,94,1,94,
      1,94,1,94,1,95,1,95,1,95,1,95,1,95,1,95,1,95,1,95,3,95,1732,8,95,1,
      96,1,96,1,96,1,96,1,96,1,96,1,96,1,96,1,96,1,96,1,96,1,96,3,96,1746,
      8,96,1,97,1,97,1,97,1,97,1,97,1,98,1,98,1,98,1,98,1,98,1,98,1,98,1,
      98,3,98,1761,8,98,1,99,1,99,1,99,1,99,1,99,1,99,1,99,1,99,3,99,1771,
      8,99,1,100,1,100,1,100,1,100,1,100,1,101,1,101,1,101,1,101,1,101,1,
      102,1,102,1,102,1,102,1,102,1,103,1,103,1,103,1,103,1,103,1,103,1,
      103,1,103,3,103,1796,8,103,1,104,1,104,1,104,1,104,1,104,1,104,1,104,
      1,104,3,104,1806,8,104,1,105,1,105,1,105,1,105,1,105,1,106,1,106,1,
      106,1,106,1,106,1,106,1,106,1,106,1,106,1,106,1,106,1,106,1,106,3,
      106,1826,8,106,1,107,1,107,1,107,1,107,1,107,1,107,1,107,1,107,1,107,
      1,107,1,107,1,107,1,107,3,107,1841,8,107,1,108,1,108,1,108,1,108,1,
      108,1,108,1,108,1,108,3,108,1851,8,108,1,109,1,109,1,109,1,109,1,109,
      1,110,1,110,1,110,1,110,1,110,1,111,1,111,1,111,1,111,1,111,1,111,
      1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,
      1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,
      1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,1,111,
      1,111,1,111,1,111,3,111,1905,8,111,1,112,1,112,1,112,1,112,1,112,1,
      113,1,113,1,113,1,113,1,113,1,114,1,114,1,114,1,114,1,114,1,114,1,
      114,1,114,3,114,1925,8,114,1,115,1,115,1,115,1,115,1,116,1,116,1,116,
      1,116,1,116,1,117,1,117,1,117,1,117,1,117,1,118,1,118,1,118,1,118,
      1,119,1,119,1,119,1,119,1,119,1,119,1,119,1,119,3,119,1953,8,119,1,
      120,1,120,1,120,1,120,1,121,1,121,1,121,1,121,1,122,1,122,1,122,1,
      122,1,122,1,123,1,123,1,123,1,123,1,123,1,123,1,123,1,123,3,123,1976,
      8,123,1,124,1,124,1,124,1,124,1,124,3,124,1983,8,124,1,124,1,124,1,
      125,1,125,1,125,1,125,1,125,1,126,1,126,1,126,1,126,1,126,1,126,1,
      126,1,126,1,126,3,126,2001,8,126,1,126,1,126,1,126,1,126,1,126,1,126,
      1,126,1,126,1,126,1,126,1,126,1,126,1,126,1,126,1,126,1,126,3,126,
      2019,8,126,1,126,1,126,1,126,1,126,1,126,1,126,1,126,3,126,2028,8,
      126,1,127,1,127,1,127,1,127,1,127,1,128,1,128,1,128,1,128,1,128,1,
      128,1,128,1,128,1,128,1,128,1,128,1,128,1,128,1,128,1,128,1,128,3,
      128,2051,8,128,1,129,1,129,1,129,1,129,1,130,1,130,1,130,1,130,1,130,
      1,130,1,130,1,130,1,130,1,130,1,130,1,130,3,130,2069,8,130,1,131,1,
      131,1,131,1,131,1,131,1,131,1,131,1,131,1,131,1,131,1,131,1,131,1,
      131,1,131,1,131,1,131,3,131,2087,8,131,1,132,1,132,1,132,1,132,1,133,
      1,133,1,133,1,133,1,133,1,133,1,133,1,133,3,133,2101,8,133,1,134,1,
      134,1,134,1,134,1,134,1,134,1,134,1,134,3,134,2111,8,134,1,135,1,135,
      1,135,1,135,1,135,1,135,1,135,1,135,1,135,1,135,1,135,1,135,3,135,
      2125,8,135,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,
      136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,
      136,3,136,2148,8,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,
      1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,1,136,
      1,136,1,136,3,136,2171,8,136,1,136,1,136,1,136,1,136,1,136,1,136,1,
      136,3,136,2180,8,136,1,137,1,137,1,137,1,137,1,138,1,138,1,138,1,138,
      1,138,1,139,1,139,1,139,1,139,1,140,1,140,1,140,1,140,1,140,1,140,
      1,140,1,140,1,140,1,140,1,140,1,140,3,140,2207,8,140,1,141,1,141,1,
      141,1,141,1,141,1,141,1,141,1,141,1,141,1,141,1,141,1,141,3,141,2221,
      8,141,1,142,1,142,1,142,1,142,1,142,1,143,1,143,1,143,1,143,1,143,
      1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,
      1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,1,143,
      1,143,3,143,2256,8,143,1,144,1,144,1,144,1,144,1,144,1,144,1,144,1,
      144,3,144,2266,8,144,1,145,1,145,1,145,1,145,1,145,1,145,1,145,1,145,
      1,145,1,145,1,145,1,145,1,145,1,145,1,145,1,145,1,145,1,145,1,145,
      1,145,3,145,2288,8,145,1,146,1,146,1,146,1,146,1,146,1,147,1,147,1,
      147,1,147,1,147,1,148,1,148,1,148,1,148,1,148,3,148,2305,8,148,1,148,
      1,148,1,148,1,148,1,148,1,148,1,148,1,148,1,148,1,148,1,148,1,148,
      3,148,2319,8,148,1,149,1,149,1,149,1,149,1,149,3,149,2326,8,149,1,
      149,1,149,1,150,1,150,1,150,1,150,1,150,1,150,1,150,1,150,3,150,2338,
      8,150,1,151,1,151,1,151,1,151,1,151,1,151,1,151,1,151,3,151,2348,8,
      151,1,152,1,152,1,152,1,152,1,152,1,153,1,153,1,153,1,153,1,153,1,
      153,1,153,1,153,3,153,2363,8,153,1,154,1,154,1,154,1,154,1,154,1,154,
      1,154,1,154,3,154,2373,8,154,1,155,1,155,1,155,1,155,1,156,1,156,1,
      156,1,156,1,156,1,157,1,157,1,157,1,157,1,157,1,157,1,157,1,157,1,
      157,1,157,3,157,2394,8,157,1,158,1,158,1,158,1,158,1,159,1,159,1,159,
      1,159,1,159,1,159,1,159,1,159,1,159,1,159,3,159,2410,8,159,1,159,1,
      159,3,159,2414,8,159,1,160,1,160,1,160,1,160,1,160,1,161,1,161,1,161,
      1,161,1,161,1,161,1,161,1,161,1,161,1,161,1,161,1,161,1,161,1,161,
      1,161,1,161,1,161,3,161,2438,8,161,1,162,1,162,1,162,1,162,3,162,2444,
      8,162,1,162,1,162,1,162,1,162,1,162,1,162,3,162,2452,8,162,1,162,1,
      162,1,162,1,162,3,162,2458,8,162,1,162,1,162,3,162,2462,8,162,1,163,
      1,163,1,163,1,163,1,164,3,164,2469,8,164,1,164,1,164,1,164,1,164,1,
      164,5,164,2476,8,164,10,164,12,164,2479,9,164,3,164,2481,8,164,1,164,
      3,164,2484,8,164,1,165,1,165,3,165,2488,8,165,1,165,1,165,1,165,1,
      166,1,166,1,166,1,166,1,166,1,166,1,166,1,166,3,166,2501,8,166,1,167,
      1,167,1,167,1,167,3,167,2507,8,167,1,168,1,168,3,168,2511,8,168,1,
      169,1,169,1,170,1,170,1,170,1,170,1,170,1,170,1,170,1,170,1,170,1,
      170,1,170,1,170,3,170,2527,8,170,1,171,1,171,1,171,1,171,1,171,1,171,
      1,171,1,171,1,171,1,171,1,171,1,171,1,171,1,171,1,171,1,171,3,171,
      2545,8,171,1,172,1,172,1,172,1,172,1,172,1,172,1,172,1,172,1,172,1,
      172,1,172,1,172,3,172,2559,8,172,1,173,1,173,3,173,2563,8,173,1,174,
      1,174,1,175,1,175,1,175,1,175,1,175,1,175,1,175,1,175,1,175,1,175,
      1,175,1,175,1,175,1,175,1,175,3,175,2582,8,175,1,176,1,176,1,176,1,
      176,3,176,2588,8,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,
      3,176,2598,8,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,3,
      176,2608,8,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,1,176,
      1,176,1,176,1,176,1,176,1,176,1,176,1,176,3,176,2626,8,176,1,177,1,
      177,1,177,1,177,1,177,1,177,1,177,1,177,3,177,2636,8,177,1,178,1,178,
      1,178,1,178,1,178,1,178,1,178,1,178,1,178,1,178,1,178,1,178,1,178,
      1,178,1,178,1,178,3,178,2654,8,178,1,179,1,179,1,179,1,179,1,179,1,
      179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,
      179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,
      179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,179,1,
      179,1,179,1,179,1,179,1,179,1,179,3,179,2700,8,179,1,180,1,180,1,180,
      1,180,1,180,1,180,1,180,1,180,1,180,1,180,1,180,1,180,3,180,2714,8,
      180,1,181,1,181,1,181,1,181,1,181,1,181,1,181,1,181,1,181,1,181,1,
      181,1,181,1,181,1,181,1,181,1,181,3,181,2732,8,181,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,1,182,
      1,182,1,182,1,182,1,182,3,182,2950,8,182,1,183,1,183,1,183,1,183,1,
      183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,
      183,1,183,1,183,1,183,1,183,1,183,1,183,3,183,2974,8,183,1,183,1,183,
      1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,1,183,
      1,183,1,183,1,183,1,183,1,183,1,183,5,183,2995,8,183,10,183,12,183,
      2998,9,183,1,184,1,184,1,184,1,184,1,184,1,184,1,184,1,184,3,184,3008,
      8,184,1,185,1,185,1,186,1,186,1,187,1,187,3,187,3016,8,187,1,188,1,
      188,1,189,1,189,1,189,1,189,3,189,3024,8,189,1,189,1,189,1,189,1,189,
      1,190,1,190,1,190,1,190,3,190,3034,8,190,1,190,1,190,1,190,1,190,1,
      191,1,191,1,191,1,191,3,191,3044,8,191,1,191,1,191,1,191,1,191,1,191,
      1,191,1,191,1,191,3,191,3054,8,191,1,191,1,191,1,191,1,191,3,191,3060,
      8,191,1,192,1,192,1,192,1,192,3,192,3066,8,192,1,192,1,192,1,192,1,
      192,1,193,1,193,1,193,1,193,3,193,3076,8,193,1,193,1,193,1,193,1,193,
      1,194,1,194,1,194,1,194,3,194,3086,8,194,1,194,1,194,1,194,1,194,1,
      195,1,195,1,195,1,195,3,195,3096,8,195,1,195,1,195,1,195,1,195,1,196,
      1,196,1,196,1,196,3,196,3106,8,196,1,196,1,196,1,196,1,196,1,196,1,
      196,1,197,1,197,1,197,1,197,3,197,3118,8,197,1,197,1,197,1,197,1,197,
      1,197,1,197,1,198,1,198,1,198,1,198,3,198,3130,8,198,1,198,1,198,1,
      198,1,198,1,198,1,198,1,199,1,199,1,199,1,199,3,199,3142,8,199,1,199,
      1,199,1,199,1,199,1,199,1,199,3,199,3150,8,199,1,199,1,199,1,199,1,
      199,3,199,3156,8,199,1,200,1,200,1,200,1,200,3,200,3162,8,200,1,200,
      1,200,1,200,1,200,1,200,1,200,3,200,3170,8,200,1,200,1,200,1,200,1,
      200,3,200,3176,8,200,1,201,1,201,1,201,1,201,3,201,3182,8,201,1,201,
      1,201,1,201,1,201,1,202,1,202,1,202,1,202,3,202,3192,8,202,1,202,1,
      202,1,202,1,202,1,203,1,203,1,203,1,203,3,203,3202,8,203,1,203,1,203,
      1,203,1,203,1,204,1,204,1,204,1,204,3,204,3212,8,204,1,204,1,204,1,
      204,1,204,1,205,1,205,1,205,1,205,3,205,3222,8,205,1,205,1,205,1,205,
      1,205,1,206,1,206,1,206,1,206,3,206,3232,8,206,1,206,1,206,1,206,1,
      206,1,207,1,207,1,207,1,207,3,207,3242,8,207,1,207,1,207,1,207,1,207,
      1,208,1,208,1,208,1,208,3,208,3252,8,208,1,208,1,208,1,208,1,208,1,
      209,1,209,1,209,1,209,3,209,3262,8,209,1,209,1,209,1,209,1,209,1,210,
      1,210,1,210,1,210,1,211,1,211,1,211,1,211,1,212,1,212,1,212,1,212,
      1,213,1,213,1,213,1,213,1,214,1,214,1,214,1,214,1,214,1,214,1,214,
      1,214,3,214,3292,8,214,1,215,1,215,1,215,1,215,1,216,1,216,1,216,1,
      216,1,217,1,217,1,217,1,217,1,218,1,218,1,218,1,218,1,218,1,218,1,
      218,3,218,3313,8,218,1,219,1,219,1,219,3,219,3318,8,219,1,220,1,220,
      1,220,3,220,3323,8,220,1,221,1,221,1,221,3,221,3328,8,221,1,222,1,
      222,1,222,1,222,1,222,3,222,3335,8,222,1,223,1,223,1,223,1,223,1,223,
      1,223,1,223,1,223,1,223,3,223,3346,8,223,1,224,1,224,3,224,3350,8,
      224,1,225,1,225,1,225,3,225,3355,8,225,1,226,1,226,1,226,1,226,1,227,
      1,227,1,227,1,227,1,228,1,228,1,228,1,228,1,229,1,229,1,229,1,229,
      1,230,1,230,1,230,1,230,1,231,1,231,1,231,1,231,1,232,1,232,1,232,
      1,232,1,233,1,233,1,233,1,233,1,234,1,234,1,234,1,234,1,235,1,235,
      1,235,1,235,1,236,1,236,1,236,1,236,1,237,1,237,1,237,1,237,1,238,
      1,238,1,238,1,238,1,239,1,239,1,239,1,239,1,240,1,240,1,240,1,240,
      1,241,1,241,1,241,1,241,1,242,1,242,1,242,1,242,1,243,1,243,1,243,
      1,243,1,244,1,244,1,244,1,244,1,245,1,245,1,245,1,245,1,246,1,246,
      1,246,1,246,1,247,1,247,1,247,1,247,1,248,1,248,1,248,1,248,1,249,
      1,249,1,249,1,249,1,250,1,250,1,250,1,250,1,251,1,251,1,251,1,251,
      1,252,1,252,1,252,1,252,1,253,1,253,1,253,1,253,1,254,1,254,1,254,
      1,254,1,255,1,255,1,256,1,256,1,257,1,257,1,258,1,258,1,259,1,259,
      1,260,1,260,1,261,1,261,3,261,3487,8,261,1,262,1,262,3,262,3491,8,
      262,1,263,1,263,3,263,3495,8,263,1,264,1,264,3,264,3499,8,264,1,265,
      1,265,1,265,5,265,3504,8,265,10,265,12,265,3507,9,265,3,265,3509,8,
      265,1,266,1,266,3,266,3513,8,266,1,267,1,267,3,267,3517,8,267,1,268,
      1,268,1,268,5,268,3522,8,268,10,268,12,268,3525,9,268,3,268,3527,8,
      268,1,269,1,269,3,269,3531,8,269,1,270,1,270,3,270,3535,8,270,1,271,
      1,271,3,271,3539,8,271,1,272,3,272,3542,8,272,1,273,1,273,1,273,5,
      273,3547,8,273,10,273,12,273,3550,9,273,1,274,3,274,3553,8,274,1,275,
      1,275,1,275,5,275,3558,8,275,10,275,12,275,3561,9,275,1,276,3,276,
      3564,8,276,1,277,1,277,1,277,5,277,3569,8,277,10,277,12,277,3572,9,
      277,1,278,1,278,1,278,1,278,5,278,3578,8,278,10,278,12,278,3581,9,
      278,3,278,3583,8,278,1,278,1,278,1,279,3,279,3588,8,279,1,280,1,280,
      1,280,5,280,3593,8,280,10,280,12,280,3596,9,280,1,281,1,281,3,281,
      3600,8,281,1,282,1,282,1,282,1,282,1,282,1,282,1,282,1,282,1,282,1,
      282,3,282,3612,8,282,1,283,1,283,1,283,1,283,5,283,3618,8,283,10,283,
      12,283,3621,9,283,3,283,3623,8,283,1,283,1,283,1,284,1,284,1,284,5,
      284,3630,8,284,10,284,12,284,3633,9,284,3,284,3635,8,284,1,285,1,285,
      1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,
      1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,1,285,3,285,3659,8,
      285,1,286,1,286,1,286,1,286,1,286,1,286,1,286,5,286,3668,8,286,10,
      286,12,286,3671,9,286,1,286,1,286,3,286,3675,8,286,1,287,1,287,1,287,
      1,287,1,287,3,287,3682,8,287,1,287,1,287,1,287,1,287,1,287,3,287,3689,
      8,287,1,287,1,287,1,287,1,287,1,287,3,287,3696,8,287,1,287,1,287,1,
      287,1,287,1,287,3,287,3703,8,287,1,287,1,287,1,287,1,287,1,287,3,287,
      3710,8,287,1,287,1,287,1,287,1,287,1,287,3,287,3717,8,287,1,287,1,
      287,1,287,1,287,1,287,3,287,3724,8,287,1,287,1,287,3,287,3728,8,287,
      3,287,3730,8,287,1,288,1,288,1,288,1,288,1,289,1,289,1,290,1,290,3,
      290,3740,8,290,1,291,1,291,1,292,1,292,1,292,3,292,3747,8,292,1,293,
      1,293,3,293,3751,8,293,1,294,1,294,1,295,1,295,1,295,1,295,1,295,1,
      295,1,295,1,295,1,295,1,295,1,295,1,295,1,295,1,295,1,295,1,295,3,
      295,3771,8,295,1,296,1,296,1,297,1,297,1,298,1,298,1,299,1,299,1,299,
      1,299,1,299,1,299,1,299,1,299,3,299,3787,8,299,1,300,1,300,1,301,1,
      301,1,301,1,301,1,301,1,301,1,301,3,301,3798,8,301,1,301,1,301,1,302,
      1,302,1,302,1,302,1,302,1,303,1,303,1,304,1,304,1,305,1,305,1,306,
      1,306,1,306,0,4,2,6,50,366,307,0,2,4,6,8,10,12,14,16,18,20,22,24,26,
      28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,
      72,74,76,78,80,82,84,86,88,90,92,94,96,98,100,102,104,106,108,110,
      112,114,116,118,120,122,124,126,128,130,132,134,136,138,140,142,144,
      146,148,150,152,154,156,158,160,162,164,166,168,170,172,174,176,178,
      180,182,184,186,188,190,192,194,196,198,200,202,204,206,208,210,212,
      214,216,218,220,222,224,226,228,230,232,234,236,238,240,242,244,246,
      248,250,252,254,256,258,260,262,264,266,268,270,272,274,276,278,280,
      282,284,286,288,290,292,294,296,298,300,302,304,306,308,310,312,314,
      316,318,320,322,324,326,328,330,332,334,336,338,340,342,344,346,348,
      350,352,354,356,358,360,362,364,366,368,370,372,374,376,378,380,382,
      384,386,388,390,392,394,396,398,400,402,404,406,408,410,412,414,416,
      418,420,422,424,426,428,430,432,434,436,438,440,442,444,446,448,450,
      452,454,456,458,460,462,464,466,468,470,472,474,476,478,480,482,484,
      486,488,490,492,494,496,498,500,502,504,506,508,510,512,514,516,518,
      520,522,524,526,528,530,532,534,536,538,540,542,544,546,548,550,552,
      554,556,558,560,562,564,566,568,570,572,574,576,578,580,582,584,586,
      588,590,592,594,596,598,600,602,604,606,608,610,612,0,6,4,0,117,117,
      138,138,141,141,283,283,5,0,29,29,97,97,121,121,196,196,257,257,1,
      0,305,308,2,0,89,89,270,270,2,0,127,127,303,303,2,0,1,300,319,319,
      4159,0,614,1,0,0,0,2,641,1,0,0,0,4,653,1,0,0,0,6,660,1,0,0,0,8,691,
      1,0,0,0,10,703,1,0,0,0,12,712,1,0,0,0,14,714,1,0,0,0,16,719,1,0,0,
      0,18,735,1,0,0,0,20,753,1,0,0,0,22,755,1,0,0,0,24,764,1,0,0,0,26,785,
      1,0,0,0,28,797,1,0,0,0,30,809,1,0,0,0,32,824,1,0,0,0,34,826,1,0,0,
      0,36,831,1,0,0,0,38,836,1,0,0,0,40,841,1,0,0,0,42,856,1,0,0,0,44,868,
      1,0,0,0,46,901,1,0,0,0,48,903,1,0,0,0,50,908,1,0,0,0,52,923,1,0,0,
      0,54,925,1,0,0,0,56,1064,1,0,0,0,58,1066,1,0,0,0,60,1071,1,0,0,0,62,
      1086,1,0,0,0,64,1101,1,0,0,0,66,1103,1,0,0,0,68,1108,1,0,0,0,70,1113,
      1,0,0,0,72,1118,1,0,0,0,74,1123,1,0,0,0,76,1132,1,0,0,0,78,1136,1,
      0,0,0,80,1148,1,0,0,0,82,1158,1,0,0,0,84,1173,1,0,0,0,86,1175,1,0,
      0,0,88,1180,1,0,0,0,90,1185,1,0,0,0,92,1189,1,0,0,0,94,1248,1,0,0,
      0,96,1278,1,0,0,0,98,1280,1,0,0,0,100,1331,1,0,0,0,102,1333,1,0,0,
      0,104,1338,1,0,0,0,106,1343,1,0,0,0,108,1362,1,0,0,0,110,1364,1,0,
      0,0,112,1369,1,0,0,0,114,1373,1,0,0,0,116,1386,1,0,0,0,118,1388,1,
      0,0,0,120,1392,1,0,0,0,122,1409,1,0,0,0,124,1425,1,0,0,0,126,1427,
      1,0,0,0,128,1432,1,0,0,0,130,1436,1,0,0,0,132,1441,1,0,0,0,134,1445,
      1,0,0,0,136,1449,1,0,0,0,138,1467,1,0,0,0,140,1477,1,0,0,0,142,1489,
      1,0,0,0,144,1491,1,0,0,0,146,1506,1,0,0,0,148,1508,1,0,0,0,150,1523,
      1,0,0,0,152,1533,1,0,0,0,154,1543,1,0,0,0,156,1596,1,0,0,0,158,1612,
      1,0,0,0,160,1628,1,0,0,0,162,1644,1,0,0,0,164,1646,1,0,0,0,166,1665,
      1,0,0,0,168,1667,1,0,0,0,170,1671,1,0,0,0,172,1675,1,0,0,0,174,1680,
      1,0,0,0,176,1685,1,0,0,0,178,1690,1,0,0,0,180,1694,1,0,0,0,182,1698,
      1,0,0,0,184,1713,1,0,0,0,186,1715,1,0,0,0,188,1719,1,0,0,0,190,1731,
      1,0,0,0,192,1745,1,0,0,0,194,1747,1,0,0,0,196,1760,1,0,0,0,198,1770,
      1,0,0,0,200,1772,1,0,0,0,202,1777,1,0,0,0,204,1782,1,0,0,0,206,1795,
      1,0,0,0,208,1805,1,0,0,0,210,1807,1,0,0,0,212,1825,1,0,0,0,214,1840,
      1,0,0,0,216,1850,1,0,0,0,218,1852,1,0,0,0,220,1857,1,0,0,0,222,1904,
      1,0,0,0,224,1906,1,0,0,0,226,1911,1,0,0,0,228,1924,1,0,0,0,230,1926,
      1,0,0,0,232,1930,1,0,0,0,234,1935,1,0,0,0,236,1940,1,0,0,0,238,1952,
      1,0,0,0,240,1954,1,0,0,0,242,1958,1,0,0,0,244,1962,1,0,0,0,246,1975,
      1,0,0,0,248,1977,1,0,0,0,250,1986,1,0,0,0,252,2027,1,0,0,0,254,2029,
      1,0,0,0,256,2050,1,0,0,0,258,2052,1,0,0,0,260,2068,1,0,0,0,262,2086,
      1,0,0,0,264,2088,1,0,0,0,266,2100,1,0,0,0,268,2110,1,0,0,0,270,2124,
      1,0,0,0,272,2179,1,0,0,0,274,2181,1,0,0,0,276,2185,1,0,0,0,278,2190,
      1,0,0,0,280,2206,1,0,0,0,282,2220,1,0,0,0,284,2222,1,0,0,0,286,2255,
      1,0,0,0,288,2265,1,0,0,0,290,2287,1,0,0,0,292,2289,1,0,0,0,294,2294,
      1,0,0,0,296,2318,1,0,0,0,298,2320,1,0,0,0,300,2337,1,0,0,0,302,2347,
      1,0,0,0,304,2349,1,0,0,0,306,2362,1,0,0,0,308,2372,1,0,0,0,310,2374,
      1,0,0,0,312,2378,1,0,0,0,314,2393,1,0,0,0,316,2395,1,0,0,0,318,2413,
      1,0,0,0,320,2415,1,0,0,0,322,2437,1,0,0,0,324,2461,1,0,0,0,326,2463,
      1,0,0,0,328,2468,1,0,0,0,330,2487,1,0,0,0,332,2500,1,0,0,0,334,2506,
      1,0,0,0,336,2510,1,0,0,0,338,2512,1,0,0,0,340,2526,1,0,0,0,342,2544,
      1,0,0,0,344,2558,1,0,0,0,346,2562,1,0,0,0,348,2564,1,0,0,0,350,2581,
      1,0,0,0,352,2625,1,0,0,0,354,2635,1,0,0,0,356,2653,1,0,0,0,358,2699,
      1,0,0,0,360,2713,1,0,0,0,362,2731,1,0,0,0,364,2949,1,0,0,0,366,2973,
      1,0,0,0,368,3007,1,0,0,0,370,3009,1,0,0,0,372,3011,1,0,0,0,374,3015,
      1,0,0,0,376,3017,1,0,0,0,378,3023,1,0,0,0,380,3033,1,0,0,0,382,3059,
      1,0,0,0,384,3065,1,0,0,0,386,3075,1,0,0,0,388,3085,1,0,0,0,390,3095,
      1,0,0,0,392,3105,1,0,0,0,394,3117,1,0,0,0,396,3129,1,0,0,0,398,3155,
      1,0,0,0,400,3175,1,0,0,0,402,3181,1,0,0,0,404,3191,1,0,0,0,406,3201,
      1,0,0,0,408,3211,1,0,0,0,410,3221,1,0,0,0,412,3231,1,0,0,0,414,3241,
      1,0,0,0,416,3251,1,0,0,0,418,3261,1,0,0,0,420,3267,1,0,0,0,422,3271,
      1,0,0,0,424,3275,1,0,0,0,426,3279,1,0,0,0,428,3291,1,0,0,0,430,3293,
      1,0,0,0,432,3297,1,0,0,0,434,3301,1,0,0,0,436,3312,1,0,0,0,438,3317,
      1,0,0,0,440,3322,1,0,0,0,442,3327,1,0,0,0,444,3334,1,0,0,0,446,3345,
      1,0,0,0,448,3349,1,0,0,0,450,3354,1,0,0,0,452,3356,1,0,0,0,454,3360,
      1,0,0,0,456,3364,1,0,0,0,458,3368,1,0,0,0,460,3372,1,0,0,0,462,3376,
      1,0,0,0,464,3380,1,0,0,0,466,3384,1,0,0,0,468,3388,1,0,0,0,470,3392,
      1,0,0,0,472,3396,1,0,0,0,474,3400,1,0,0,0,476,3404,1,0,0,0,478,3408,
      1,0,0,0,480,3412,1,0,0,0,482,3416,1,0,0,0,484,3420,1,0,0,0,486,3424,
      1,0,0,0,488,3428,1,0,0,0,490,3432,1,0,0,0,492,3436,1,0,0,0,494,3440,
      1,0,0,0,496,3444,1,0,0,0,498,3448,1,0,0,0,500,3452,1,0,0,0,502,3456,
      1,0,0,0,504,3460,1,0,0,0,506,3464,1,0,0,0,508,3468,1,0,0,0,510,3472,
      1,0,0,0,512,3474,1,0,0,0,514,3476,1,0,0,0,516,3478,1,0,0,0,518,3480,
      1,0,0,0,520,3482,1,0,0,0,522,3486,1,0,0,0,524,3490,1,0,0,0,526,3494,
      1,0,0,0,528,3498,1,0,0,0,530,3508,1,0,0,0,532,3512,1,0,0,0,534,3516,
      1,0,0,0,536,3526,1,0,0,0,538,3530,1,0,0,0,540,3534,1,0,0,0,542,3538,
      1,0,0,0,544,3541,1,0,0,0,546,3543,1,0,0,0,548,3552,1,0,0,0,550,3554,
      1,0,0,0,552,3563,1,0,0,0,554,3565,1,0,0,0,556,3573,1,0,0,0,558,3587,
      1,0,0,0,560,3589,1,0,0,0,562,3599,1,0,0,0,564,3611,1,0,0,0,566,3613,
      1,0,0,0,568,3634,1,0,0,0,570,3658,1,0,0,0,572,3674,1,0,0,0,574,3729,
      1,0,0,0,576,3731,1,0,0,0,578,3735,1,0,0,0,580,3739,1,0,0,0,582,3741,
      1,0,0,0,584,3746,1,0,0,0,586,3750,1,0,0,0,588,3752,1,0,0,0,590,3770,
      1,0,0,0,592,3772,1,0,0,0,594,3774,1,0,0,0,596,3776,1,0,0,0,598,3786,
      1,0,0,0,600,3788,1,0,0,0,602,3790,1,0,0,0,604,3801,1,0,0,0,606,3806,
      1,0,0,0,608,3808,1,0,0,0,610,3810,1,0,0,0,612,3812,1,0,0,0,614,621,
      3,2,1,0,615,617,5,315,0,0,616,615,1,0,0,0,616,617,1,0,0,0,617,618,
      1,0,0,0,618,620,3,2,1,0,619,616,1,0,0,0,620,623,1,0,0,0,621,619,1,
      0,0,0,621,622,1,0,0,0,622,625,1,0,0,0,623,621,1,0,0,0,624,626,5,315,
      0,0,625,624,1,0,0,0,625,626,1,0,0,0,626,627,1,0,0,0,627,628,5,0,0,
      1,628,1,1,0,0,0,629,630,6,1,-1,0,630,642,3,6,3,0,631,632,3,6,3,0,632,
      633,5,317,0,0,633,634,3,8,4,0,634,642,1,0,0,0,635,642,3,10,5,0,636,
      637,3,10,5,0,637,638,5,317,0,0,638,639,3,368,184,0,639,642,1,0,0,0,
      640,642,3,4,2,0,641,629,1,0,0,0,641,631,1,0,0,0,641,635,1,0,0,0,641,
      636,1,0,0,0,641,640,1,0,0,0,642,650,1,0,0,0,643,644,10,2,0,0,644,645,
      5,317,0,0,645,646,5,263,0,0,646,647,5,309,0,0,647,649,5,310,0,0,648,
      643,1,0,0,0,649,652,1,0,0,0,650,648,1,0,0,0,650,651,1,0,0,0,651,3,
      1,0,0,0,652,650,1,0,0,0,653,654,5,308,0,0,654,5,1,0,0,0,655,656,6,
      3,-1,0,656,661,5,319,0,0,657,658,5,319,0,0,658,659,5,317,0,0,659,661,
      3,12,6,0,660,655,1,0,0,0,660,657,1,0,0,0,661,667,1,0,0,0,662,663,10,
      1,0,0,663,664,5,317,0,0,664,666,3,12,6,0,665,662,1,0,0,0,666,669,1,
      0,0,0,667,665,1,0,0,0,667,668,1,0,0,0,668,7,1,0,0,0,669,667,1,0,0,
      0,670,671,5,273,0,0,671,672,5,309,0,0,672,673,5,310,0,0,673,674,5,
      317,0,0,674,675,5,17,0,0,675,676,5,309,0,0,676,692,5,310,0,0,677,678,
      5,273,0,0,678,679,5,309,0,0,679,680,5,310,0,0,680,681,5,317,0,0,681,
      682,5,46,0,0,682,683,5,309,0,0,683,692,5,310,0,0,684,685,5,273,0,0,
      685,686,5,309,0,0,686,687,5,310,0,0,687,688,5,317,0,0,688,689,5,225,
      0,0,689,690,5,309,0,0,690,692,5,310,0,0,691,670,1,0,0,0,691,677,1,
      0,0,0,691,684,1,0,0,0,692,9,1,0,0,0,693,694,3,6,3,0,694,695,5,317,
      0,0,695,696,3,28,14,0,696,704,1,0,0,0,697,698,3,6,3,0,698,699,5,317,
      0,0,699,700,3,28,14,0,700,701,5,317,0,0,701,702,3,50,25,0,702,704,
      1,0,0,0,703,693,1,0,0,0,703,697,1,0,0,0,704,11,1,0,0,0,705,713,3,14,
      7,0,706,713,3,16,8,0,707,713,3,18,9,0,708,713,3,20,10,0,709,713,3,
      22,11,0,710,713,3,24,12,0,711,713,3,26,13,0,712,705,1,0,0,0,712,706,
      1,0,0,0,712,707,1,0,0,0,712,708,1,0,0,0,712,709,1,0,0,0,712,710,1,
      0,0,0,712,711,1,0,0,0,713,13,1,0,0,0,714,715,5,290,0,0,715,716,5,309,
      0,0,716,717,3,588,294,0,717,718,5,310,0,0,718,15,1,0,0,0,719,720,5,
      295,0,0,720,721,5,309,0,0,721,722,5,310,0,0,722,17,1,0,0,0,723,724,
      5,296,0,0,724,725,5,309,0,0,725,726,3,570,285,0,726,727,5,310,0,0,
      727,736,1,0,0,0,728,729,5,296,0,0,729,730,5,309,0,0,730,731,3,570,
      285,0,731,732,5,316,0,0,732,733,3,376,188,0,733,734,5,310,0,0,734,
      736,1,0,0,0,735,723,1,0,0,0,735,728,1,0,0,0,736,19,1,0,0,0,737,738,
      5,297,0,0,738,739,5,309,0,0,739,740,3,578,289,0,740,741,5,316,0,0,
      741,742,3,570,285,0,742,743,5,310,0,0,743,754,1,0,0,0,744,745,5,297,
      0,0,745,746,5,309,0,0,746,747,3,578,289,0,747,748,5,316,0,0,748,749,
      3,570,285,0,749,750,5,316,0,0,750,751,3,376,188,0,751,752,5,310,0,
      0,752,754,1,0,0,0,753,737,1,0,0,0,753,744,1,0,0,0,754,21,1,0,0,0,755,
      756,5,298,0,0,756,757,5,309,0,0,757,760,3,328,164,0,758,759,5,316,
      0,0,759,761,3,544,272,0,760,758,1,0,0,0,760,761,1,0,0,0,761,762,1,
      0,0,0,762,763,5,310,0,0,763,23,1,0,0,0,764,765,5,294,0,0,765,766,5,
      309,0,0,766,769,3,608,304,0,767,768,5,316,0,0,768,770,3,548,274,0,
      769,767,1,0,0,0,769,770,1,0,0,0,770,771,1,0,0,0,771,772,5,310,0,0,
      772,25,1,0,0,0,773,774,5,289,0,0,774,775,5,309,0,0,775,776,3,578,289,
      0,776,777,5,310,0,0,777,786,1,0,0,0,778,779,5,289,0,0,779,780,5,309,
      0,0,780,781,3,578,289,0,781,782,5,316,0,0,782,783,3,570,285,0,783,
      784,5,310,0,0,784,786,1,0,0,0,785,773,1,0,0,0,785,778,1,0,0,0,786,
      27,1,0,0,0,787,798,3,30,15,0,788,798,3,32,16,0,789,798,3,34,17,0,790,
      798,3,36,18,0,791,798,3,44,22,0,792,798,3,42,21,0,793,798,3,38,19,
      0,794,798,3,40,20,0,795,798,3,46,23,0,796,798,3,48,24,0,797,787,1,
      0,0,0,797,788,1,0,0,0,797,789,1,0,0,0,797,790,1,0,0,0,797,791,1,0,
      0,0,797,792,1,0,0,0,797,793,1,0,0,0,797,794,1,0,0,0,797,795,1,0,0,
      0,797,796,1,0,0,0,798,29,1,0,0,0,799,800,5,2,0,0,800,801,5,309,0,0,
      801,802,3,526,263,0,802,803,5,310,0,0,803,810,1,0,0,0,804,805,5,2,
      0,0,805,806,5,309,0,0,806,807,3,52,26,0,807,808,5,310,0,0,808,810,
      1,0,0,0,809,799,1,0,0,0,809,804,1,0,0,0,810,31,1,0,0,0,811,812,5,3,
      0,0,812,813,5,309,0,0,813,825,5,310,0,0,814,815,5,3,0,0,815,816,5,
      309,0,0,816,817,3,526,263,0,817,818,5,310,0,0,818,825,1,0,0,0,819,
      820,5,3,0,0,820,821,5,309,0,0,821,822,3,52,26,0,822,823,5,310,0,0,
      823,825,1,0,0,0,824,811,1,0,0,0,824,814,1,0,0,0,824,819,1,0,0,0,825,
      33,1,0,0,0,826,827,5,78,0,0,827,828,5,309,0,0,828,829,3,536,268,0,
      829,830,5,310,0,0,830,35,1,0,0,0,831,832,5,280,0,0,832,833,5,309,0,
      0,833,834,3,536,268,0,834,835,5,310,0,0,835,37,1,0,0,0,836,837,5,128,
      0,0,837,838,5,309,0,0,838,839,3,558,279,0,839,840,5,310,0,0,840,39,
      1,0,0,0,841,842,5,135,0,0,842,843,5,309,0,0,843,844,3,578,289,0,844,
      845,5,310,0,0,845,41,1,0,0,0,846,847,5,164,0,0,847,848,5,309,0,0,848,
      849,3,540,270,0,849,850,5,310,0,0,850,857,1,0,0,0,851,852,5,164,0,
      0,852,853,5,309,0,0,853,854,3,52,26,0,854,855,5,310,0,0,855,857,1,
      0,0,0,856,846,1,0,0,0,856,851,1,0,0,0,857,43,1,0,0,0,858,859,5,163,
      0,0,859,860,5,309,0,0,860,861,3,540,270,0,861,862,5,310,0,0,862,869,
      1,0,0,0,863,864,5,163,0,0,864,865,5,309,0,0,865,866,3,52,26,0,866,
      867,5,310,0,0,867,869,1,0,0,0,868,858,1,0,0,0,868,863,1,0,0,0,869,
      45,1,0,0,0,870,871,5,36,0,0,871,872,5,309,0,0,872,902,5,310,0,0,873,
      874,5,36,0,0,874,875,5,309,0,0,875,876,3,578,289,0,876,877,5,310,0,
      0,877,902,1,0,0,0,878,879,5,36,0,0,879,880,5,309,0,0,880,881,3,578,
      289,0,881,882,5,316,0,0,882,883,3,538,269,0,883,884,5,310,0,0,884,
      902,1,0,0,0,885,886,5,36,0,0,886,887,5,309,0,0,887,888,3,578,289,0,
      888,889,5,316,0,0,889,890,3,52,26,0,890,891,5,310,0,0,891,902,1,0,
      0,0,892,893,5,36,0,0,893,894,5,309,0,0,894,895,3,578,289,0,895,896,
      5,316,0,0,896,897,3,538,269,0,897,898,5,316,0,0,898,899,3,52,26,0,
      899,900,5,310,0,0,900,902,1,0,0,0,901,870,1,0,0,0,901,873,1,0,0,0,
      901,878,1,0,0,0,901,885,1,0,0,0,901,892,1,0,0,0,902,47,1,0,0,0,903,
      904,5,275,0,0,904,905,5,309,0,0,905,906,3,552,276,0,906,907,5,310,
      0,0,907,49,1,0,0,0,908,909,6,25,-1,0,909,910,3,56,28,0,910,916,1,0,
      0,0,911,912,10,1,0,0,912,913,5,317,0,0,913,915,3,56,28,0,914,911,1,
      0,0,0,915,918,1,0,0,0,916,914,1,0,0,0,916,917,1,0,0,0,917,51,1,0,0,
      0,918,916,1,0,0,0,919,924,3,50,25,0,920,921,5,320,0,0,921,922,5,317,
      0,0,922,924,3,50,25,0,923,919,1,0,0,0,923,920,1,0,0,0,924,53,1,0,0,
      0,925,926,3,10,5,0,926,927,5,317,0,0,927,928,3,368,184,0,928,55,1,
      0,0,0,929,1065,3,58,29,0,930,1065,3,60,30,0,931,1065,3,62,31,0,932,
      1065,3,64,32,0,933,1065,3,214,107,0,934,1065,3,212,106,0,935,1065,
      3,66,33,0,936,1065,3,68,34,0,937,1065,3,70,35,0,938,1065,3,72,36,0,
      939,1065,3,74,37,0,940,1065,3,84,42,0,941,1065,3,86,43,0,942,1065,
      3,88,44,0,943,1065,3,90,45,0,944,1065,3,92,46,0,945,1065,3,94,47,0,
      946,1065,3,98,49,0,947,1065,3,100,50,0,948,1065,3,102,51,0,949,1065,
      3,104,52,0,950,1065,3,110,55,0,951,1065,3,112,56,0,952,1065,3,114,
      57,0,953,1065,3,116,58,0,954,1065,3,118,59,0,955,1065,3,124,62,0,956,
      1065,3,126,63,0,957,1065,3,128,64,0,958,1065,3,130,65,0,959,1065,3,
      132,66,0,960,1065,3,136,68,0,961,1065,3,138,69,0,962,1065,3,142,71,
      0,963,1065,3,144,72,0,964,1065,3,146,73,0,965,1065,3,150,75,0,966,
      1065,3,152,76,0,967,1065,3,154,77,0,968,1065,3,156,78,0,969,1065,3,
      158,79,0,970,1065,3,160,80,0,971,1065,3,162,81,0,972,1065,3,164,82,
      0,973,1065,3,166,83,0,974,1065,3,168,84,0,975,1065,3,170,85,0,976,
      1065,3,172,86,0,977,1065,3,174,87,0,978,1065,3,176,88,0,979,1065,3,
      178,89,0,980,1065,3,180,90,0,981,1065,3,182,91,0,982,1065,3,184,92,
      0,983,1065,3,186,93,0,984,1065,3,188,94,0,985,1065,3,192,96,0,986,
      1065,3,194,97,0,987,1065,3,196,98,0,988,1065,3,200,100,0,989,1065,
      3,202,101,0,990,1065,3,204,102,0,991,1065,3,206,103,0,992,1065,3,208,
      104,0,993,1065,3,216,108,0,994,1065,3,218,109,0,995,1065,3,220,110,
      0,996,1065,3,222,111,0,997,1065,3,224,112,0,998,1065,3,226,113,0,999,
      1065,3,228,114,0,1000,1065,3,230,115,0,1001,1065,3,232,116,0,1002,
      1065,3,234,117,0,1003,1065,3,236,118,0,1004,1065,3,238,119,0,1005,
      1065,3,240,120,0,1006,1065,3,242,121,0,1007,1065,3,246,123,0,1008,
      1065,3,248,124,0,1009,1065,3,250,125,0,1010,1065,3,252,126,0,1011,
      1065,3,254,127,0,1012,1065,3,256,128,0,1013,1065,3,258,129,0,1014,
      1065,3,260,130,0,1015,1065,3,268,134,0,1016,1065,3,270,135,0,1017,
      1065,3,272,136,0,1018,1065,3,106,53,0,1019,1065,3,244,122,0,1020,1065,
      3,210,105,0,1021,1065,3,274,137,0,1022,1065,3,276,138,0,1023,1065,
      3,278,139,0,1024,1065,3,280,140,0,1025,1065,3,284,142,0,1026,1065,
      3,288,144,0,1027,1065,3,290,145,0,1028,1065,3,140,70,0,1029,1065,3,
      292,146,0,1030,1065,3,294,147,0,1031,1065,3,296,148,0,1032,1065,3,
      298,149,0,1033,1065,3,304,152,0,1034,1065,3,306,153,0,1035,1065,3,
      310,155,0,1036,1065,3,312,156,0,1037,1065,3,314,157,0,1038,1065,3,
      316,158,0,1039,1065,3,318,159,0,1040,1065,3,320,160,0,1041,1065,3,
      322,161,0,1042,1065,3,324,162,0,1043,1065,3,326,163,0,1044,1065,3,
      134,67,0,1045,1065,3,96,48,0,1046,1065,3,108,54,0,1047,1065,3,82,41,
      0,1048,1065,3,148,74,0,1049,1065,3,302,151,0,1050,1065,3,300,150,0,
      1051,1065,3,190,95,0,1052,1065,3,308,154,0,1053,1065,3,198,99,0,1054,
      1065,3,266,133,0,1055,1065,3,264,132,0,1056,1065,3,262,131,0,1057,
      1065,3,282,141,0,1058,1065,3,286,143,0,1059,1065,3,76,38,0,1060,1065,
      3,78,39,0,1061,1065,3,120,60,0,1062,1065,3,122,61,0,1063,1065,3,80,
      40,0,1064,929,1,0,0,0,1064,930,1,0,0,0,1064,931,1,0,0,0,1064,932,1,
      0,0,0,1064,933,1,0,0,0,1064,934,1,0,0,0,1064,935,1,0,0,0,1064,936,
      1,0,0,0,1064,937,1,0,0,0,1064,938,1,0,0,0,1064,939,1,0,0,0,1064,940,
      1,0,0,0,1064,941,1,0,0,0,1064,942,1,0,0,0,1064,943,1,0,0,0,1064,944,
      1,0,0,0,1064,945,1,0,0,0,1064,946,1,0,0,0,1064,947,1,0,0,0,1064,948,
      1,0,0,0,1064,949,1,0,0,0,1064,950,1,0,0,0,1064,951,1,0,0,0,1064,952,
      1,0,0,0,1064,953,1,0,0,0,1064,954,1,0,0,0,1064,955,1,0,0,0,1064,956,
      1,0,0,0,1064,957,1,0,0,0,1064,958,1,0,0,0,1064,959,1,0,0,0,1064,960,
      1,0,0,0,1064,961,1,0,0,0,1064,962,1,0,0,0,1064,963,1,0,0,0,1064,964,
      1,0,0,0,1064,965,1,0,0,0,1064,966,1,0,0,0,1064,967,1,0,0,0,1064,968,
      1,0,0,0,1064,969,1,0,0,0,1064,970,1,0,0,0,1064,971,1,0,0,0,1064,972,
      1,0,0,0,1064,973,1,0,0,0,1064,974,1,0,0,0,1064,975,1,0,0,0,1064,976,
      1,0,0,0,1064,977,1,0,0,0,1064,978,1,0,0,0,1064,979,1,0,0,0,1064,980,
      1,0,0,0,1064,981,1,0,0,0,1064,982,1,0,0,0,1064,983,1,0,0,0,1064,984,
      1,0,0,0,1064,985,1,0,0,0,1064,986,1,0,0,0,1064,987,1,0,0,0,1064,988,
      1,0,0,0,1064,989,1,0,0,0,1064,990,1,0,0,0,1064,991,1,0,0,0,1064,992,
      1,0,0,0,1064,993,1,0,0,0,1064,994,1,0,0,0,1064,995,1,0,0,0,1064,996,
      1,0,0,0,1064,997,1,0,0,0,1064,998,1,0,0,0,1064,999,1,0,0,0,1064,1000,
      1,0,0,0,1064,1001,1,0,0,0,1064,1002,1,0,0,0,1064,1003,1,0,0,0,1064,
      1004,1,0,0,0,1064,1005,1,0,0,0,1064,1006,1,0,0,0,1064,1007,1,0,0,0,
      1064,1008,1,0,0,0,1064,1009,1,0,0,0,1064,1010,1,0,0,0,1064,1011,1,
      0,0,0,1064,1012,1,0,0,0,1064,1013,1,0,0,0,1064,1014,1,0,0,0,1064,1015,
      1,0,0,0,1064,1016,1,0,0,0,1064,1017,1,0,0,0,1064,1018,1,0,0,0,1064,
      1019,1,0,0,0,1064,1020,1,0,0,0,1064,1021,1,0,0,0,1064,1022,1,0,0,0,
      1064,1023,1,0,0,0,1064,1024,1,0,0,0,1064,1025,1,0,0,0,1064,1026,1,
      0,0,0,1064,1027,1,0,0,0,1064,1028,1,0,0,0,1064,1029,1,0,0,0,1064,1030,
      1,0,0,0,1064,1031,1,0,0,0,1064,1032,1,0,0,0,1064,1033,1,0,0,0,1064,
      1034,1,0,0,0,1064,1035,1,0,0,0,1064,1036,1,0,0,0,1064,1037,1,0,0,0,
      1064,1038,1,0,0,0,1064,1039,1,0,0,0,1064,1040,1,0,0,0,1064,1041,1,
      0,0,0,1064,1042,1,0,0,0,1064,1043,1,0,0,0,1064,1044,1,0,0,0,1064,1045,
      1,0,0,0,1064,1046,1,0,0,0,1064,1047,1,0,0,0,1064,1048,1,0,0,0,1064,
      1049,1,0,0,0,1064,1050,1,0,0,0,1064,1051,1,0,0,0,1064,1052,1,0,0,0,
      1064,1053,1,0,0,0,1064,1054,1,0,0,0,1064,1055,1,0,0,0,1064,1056,1,
      0,0,0,1064,1057,1,0,0,0,1064,1058,1,0,0,0,1064,1059,1,0,0,0,1064,1060,
      1,0,0,0,1064,1061,1,0,0,0,1064,1062,1,0,0,0,1064,1063,1,0,0,0,1065,
      57,1,0,0,0,1066,1067,5,280,0,0,1067,1068,5,309,0,0,1068,1069,3,536,
      268,0,1069,1070,5,310,0,0,1070,59,1,0,0,0,1071,1072,5,78,0,0,1072,
      1073,5,309,0,0,1073,1074,3,536,268,0,1074,1075,5,310,0,0,1075,61,1,
      0,0,0,1076,1077,5,2,0,0,1077,1078,5,309,0,0,1078,1079,3,526,263,0,
      1079,1080,5,310,0,0,1080,1087,1,0,0,0,1081,1082,5,2,0,0,1082,1083,
      5,309,0,0,1083,1084,3,52,26,0,1084,1085,5,310,0,0,1085,1087,1,0,0,
      0,1086,1076,1,0,0,0,1086,1081,1,0,0,0,1087,63,1,0,0,0,1088,1089,5,
      3,0,0,1089,1090,5,309,0,0,1090,1102,5,310,0,0,1091,1092,5,3,0,0,1092,
      1093,5,309,0,0,1093,1094,3,526,263,0,1094,1095,5,310,0,0,1095,1102,
      1,0,0,0,1096,1097,5,3,0,0,1097,1098,5,309,0,0,1098,1099,3,52,26,0,
      1099,1100,5,310,0,0,1100,1102,1,0,0,0,1101,1088,1,0,0,0,1101,1091,
      1,0,0,0,1101,1096,1,0,0,0,1102,65,1,0,0,0,1103,1104,5,4,0,0,1104,1105,
      5,309,0,0,1105,1106,3,578,289,0,1106,1107,5,310,0,0,1107,67,1,0,0,
      0,1108,1109,5,5,0,0,1109,1110,5,309,0,0,1110,1111,3,366,183,0,1111,
      1112,5,310,0,0,1112,69,1,0,0,0,1113,1114,5,6,0,0,1114,1115,5,309,0,
      0,1115,1116,3,552,276,0,1116,1117,5,310,0,0,1117,71,1,0,0,0,1118,1119,
      5,7,0,0,1119,1120,5,309,0,0,1120,1121,3,366,183,0,1121,1122,5,310,
      0,0,1122,73,1,0,0,0,1123,1124,5,8,0,0,1124,1125,5,309,0,0,1125,1128,
      3,578,289,0,1126,1127,5,316,0,0,1127,1129,3,568,284,0,1128,1126,1,
      0,0,0,1128,1129,1,0,0,0,1129,1130,1,0,0,0,1130,1131,5,310,0,0,1131,
      75,1,0,0,0,1132,1133,5,9,0,0,1133,1134,5,309,0,0,1134,1135,5,310,0,
      0,1135,77,1,0,0,0,1136,1137,5,11,0,0,1137,1138,5,309,0,0,1138,1139,
      5,310,0,0,1139,79,1,0,0,0,1140,1141,5,12,0,0,1141,1142,5,309,0,0,1142,
      1149,5,310,0,0,1143,1144,5,12,0,0,1144,1145,5,309,0,0,1145,1146,3,
      364,182,0,1146,1147,5,310,0,0,1147,1149,1,0,0,0,1148,1140,1,0,0,0,
      1148,1143,1,0,0,0,1149,81,1,0,0,0,1150,1151,5,13,0,0,1151,1152,5,309,
      0,0,1152,1159,5,310,0,0,1153,1154,5,13,0,0,1154,1155,5,309,0,0,1155,
      1156,3,332,166,0,1156,1157,5,310,0,0,1157,1159,1,0,0,0,1158,1150,1,
      0,0,0,1158,1153,1,0,0,0,1159,83,1,0,0,0,1160,1161,5,15,0,0,1161,1162,
      5,309,0,0,1162,1163,3,370,185,0,1163,1164,5,310,0,0,1164,1174,1,0,
      0,0,1165,1166,5,15,0,0,1166,1167,5,309,0,0,1167,1174,5,310,0,0,1168,
      1169,5,15,0,0,1169,1170,5,309,0,0,1170,1171,3,582,291,0,1171,1172,
      5,310,0,0,1172,1174,1,0,0,0,1173,1160,1,0,0,0,1173,1165,1,0,0,0,1173,
      1168,1,0,0,0,1174,85,1,0,0,0,1175,1176,5,28,0,0,1176,1177,5,309,0,
      0,1177,1178,3,530,265,0,1178,1179,5,310,0,0,1179,87,1,0,0,0,1180,1181,
      5,30,0,0,1181,1182,5,309,0,0,1182,1183,3,530,265,0,1183,1184,5,310,
      0,0,1184,89,1,0,0,0,1185,1186,5,31,0,0,1186,1187,5,309,0,0,1187,1188,
      5,310,0,0,1188,91,1,0,0,0,1189,1190,5,32,0,0,1190,1191,5,309,0,0,1191,
      1192,3,52,26,0,1192,1193,5,310,0,0,1193,93,1,0,0,0,1194,1195,5,33,
      0,0,1195,1196,5,309,0,0,1196,1197,3,372,186,0,1197,1198,5,310,0,0,
      1198,1249,1,0,0,0,1199,1200,5,33,0,0,1200,1201,5,309,0,0,1201,1249,
      5,310,0,0,1202,1203,5,33,0,0,1203,1204,5,309,0,0,1204,1205,3,374,187,
      0,1205,1206,5,310,0,0,1206,1249,1,0,0,0,1207,1208,5,33,0,0,1208,1209,
      5,309,0,0,1209,1210,3,374,187,0,1210,1211,5,316,0,0,1211,1212,3,372,
      186,0,1212,1213,5,310,0,0,1213,1249,1,0,0,0,1214,1215,5,33,0,0,1215,
      1216,5,309,0,0,1216,1217,3,344,172,0,1217,1218,5,310,0,0,1218,1249,
      1,0,0,0,1219,1220,5,33,0,0,1220,1221,5,309,0,0,1221,1222,3,578,289,
      0,1222,1223,5,310,0,0,1223,1249,1,0,0,0,1224,1225,5,33,0,0,1225,1226,
      5,309,0,0,1226,1227,3,578,289,0,1227,1228,5,316,0,0,1228,1229,3,372,
      186,0,1229,1230,5,310,0,0,1230,1249,1,0,0,0,1231,1232,5,33,0,0,1232,
      1233,5,309,0,0,1233,1234,3,336,168,0,1234,1235,5,310,0,0,1235,1249,
      1,0,0,0,1236,1237,5,33,0,0,1237,1238,5,309,0,0,1238,1239,3,52,26,0,
      1239,1240,5,310,0,0,1240,1249,1,0,0,0,1241,1242,5,33,0,0,1242,1243,
      5,309,0,0,1243,1244,3,52,26,0,1244,1245,5,316,0,0,1245,1246,3,372,
      186,0,1246,1247,5,310,0,0,1247,1249,1,0,0,0,1248,1194,1,0,0,0,1248,
      1199,1,0,0,0,1248,1202,1,0,0,0,1248,1207,1,0,0,0,1248,1214,1,0,0,0,
      1248,1219,1,0,0,0,1248,1224,1,0,0,0,1248,1231,1,0,0,0,1248,1236,1,
      0,0,0,1248,1241,1,0,0,0,1249,95,1,0,0,0,1250,1251,5,36,0,0,1251,1252,
      5,309,0,0,1252,1253,3,578,289,0,1253,1254,5,310,0,0,1254,1279,1,0,
      0,0,1255,1256,5,36,0,0,1256,1257,5,309,0,0,1257,1258,3,578,289,0,1258,
      1259,5,316,0,0,1259,1260,3,538,269,0,1260,1261,5,310,0,0,1261,1279,
      1,0,0,0,1262,1263,5,36,0,0,1263,1264,5,309,0,0,1264,1265,3,578,289,
      0,1265,1266,5,316,0,0,1266,1267,3,52,26,0,1267,1268,5,310,0,0,1268,
      1279,1,0,0,0,1269,1270,5,36,0,0,1270,1271,5,309,0,0,1271,1272,3,578,
      289,0,1272,1273,5,316,0,0,1273,1274,3,538,269,0,1274,1275,5,316,0,
      0,1275,1276,3,52,26,0,1276,1277,5,310,0,0,1277,1279,1,0,0,0,1278,1250,
      1,0,0,0,1278,1255,1,0,0,0,1278,1262,1,0,0,0,1278,1269,1,0,0,0,1279,
      97,1,0,0,0,1280,1281,5,37,0,0,1281,1282,5,309,0,0,1282,1285,3,578,
      289,0,1283,1284,5,316,0,0,1284,1286,3,568,284,0,1285,1283,1,0,0,0,
      1285,1286,1,0,0,0,1286,1287,1,0,0,0,1287,1288,5,310,0,0,1288,99,1,
      0,0,0,1289,1290,5,41,0,0,1290,1291,5,309,0,0,1291,1292,3,374,187,0,
      1292,1293,5,310,0,0,1293,1332,1,0,0,0,1294,1295,5,41,0,0,1295,1296,
      5,309,0,0,1296,1297,3,366,183,0,1297,1298,5,316,0,0,1298,1299,3,52,
      26,0,1299,1300,5,310,0,0,1300,1332,1,0,0,0,1301,1302,5,41,0,0,1302,
      1303,5,309,0,0,1303,1304,3,366,183,0,1304,1305,5,316,0,0,1305,1306,
      3,52,26,0,1306,1307,5,316,0,0,1307,1308,3,52,26,0,1308,1309,5,310,
      0,0,1309,1332,1,0,0,0,1310,1311,5,41,0,0,1311,1312,5,309,0,0,1312,
      1313,3,52,26,0,1313,1314,5,310,0,0,1314,1332,1,0,0,0,1315,1316,5,41,
      0,0,1316,1317,5,309,0,0,1317,1318,3,52,26,0,1318,1319,5,316,0,0,1319,
      1320,3,52,26,0,1320,1321,5,310,0,0,1321,1332,1,0,0,0,1322,1323,5,41,
      0,0,1323,1324,5,309,0,0,1324,1325,3,52,26,0,1325,1326,5,316,0,0,1326,
      1327,3,52,26,0,1327,1328,5,316,0,0,1328,1329,3,52,26,0,1329,1330,5,
      310,0,0,1330,1332,1,0,0,0,1331,1289,1,0,0,0,1331,1294,1,0,0,0,1331,
      1301,1,0,0,0,1331,1310,1,0,0,0,1331,1315,1,0,0,0,1331,1322,1,0,0,0,
      1332,101,1,0,0,0,1333,1334,5,42,0,0,1334,1335,5,309,0,0,1335,1336,
      3,552,276,0,1336,1337,5,310,0,0,1337,103,1,0,0,0,1338,1339,5,43,0,
      0,1339,1340,5,309,0,0,1340,1341,3,586,293,0,1341,1342,5,310,0,0,1342,
      105,1,0,0,0,1343,1344,5,45,0,0,1344,1345,5,309,0,0,1345,1346,3,570,
      285,0,1346,1347,5,310,0,0,1347,107,1,0,0,0,1348,1349,5,48,0,0,1349,
      1350,5,309,0,0,1350,1353,3,52,26,0,1351,1352,5,316,0,0,1352,1354,3,
      552,276,0,1353,1351,1,0,0,0,1353,1354,1,0,0,0,1354,1355,1,0,0,0,1355,
      1356,5,310,0,0,1356,1363,1,0,0,0,1357,1358,5,48,0,0,1358,1359,5,309,
      0,0,1359,1360,3,568,284,0,1360,1361,5,310,0,0,1361,1363,1,0,0,0,1362,
      1348,1,0,0,0,1362,1357,1,0,0,0,1363,109,1,0,0,0,1364,1365,5,49,0,0,
      1365,1366,5,309,0,0,1366,1367,3,578,289,0,1367,1368,5,310,0,0,1368,
      111,1,0,0,0,1369,1370,5,50,0,0,1370,1371,5,309,0,0,1371,1372,5,310,
      0,0,1372,113,1,0,0,0,1373,1374,5,52,0,0,1374,1375,5,309,0,0,1375,1376,
      3,570,285,0,1376,1377,5,310,0,0,1377,115,1,0,0,0,1378,1379,5,54,0,
      0,1379,1380,5,309,0,0,1380,1387,5,310,0,0,1381,1382,5,54,0,0,1382,
      1383,5,309,0,0,1383,1384,3,332,166,0,1384,1385,5,310,0,0,1385,1387,
      1,0,0,0,1386,1378,1,0,0,0,1386,1381,1,0,0,0,1387,117,1,0,0,0,1388,
      1389,5,55,0,0,1389,1390,5,309,0,0,1390,1391,5,310,0,0,1391,119,1,0,
      0,0,1392,1393,5,57,0,0,1393,1394,5,309,0,0,1394,1395,3,362,181,0,1395,
      1396,5,316,0,0,1396,1397,3,582,291,0,1397,1398,5,310,0,0,1398,121,
      1,0,0,0,1399,1400,5,58,0,0,1400,1401,5,309,0,0,1401,1402,3,52,26,0,
      1402,1403,5,310,0,0,1403,1410,1,0,0,0,1404,1405,5,58,0,0,1405,1406,
      5,309,0,0,1406,1407,3,590,295,0,1407,1408,5,310,0,0,1408,1410,1,0,
      0,0,1409,1399,1,0,0,0,1409,1404,1,0,0,0,1410,123,1,0,0,0,1411,1412,
      5,63,0,0,1412,1413,5,309,0,0,1413,1416,3,332,166,0,1414,1415,5,316,
      0,0,1415,1417,3,568,284,0,1416,1414,1,0,0,0,1416,1417,1,0,0,0,1417,
      1418,1,0,0,0,1418,1419,5,310,0,0,1419,1426,1,0,0,0,1420,1421,5,63,
      0,0,1421,1422,5,309,0,0,1422,1423,3,568,284,0,1423,1424,5,310,0,0,
      1424,1426,1,0,0,0,1425,1411,1,0,0,0,1425,1420,1,0,0,0,1426,125,1,0,
      0,0,1427,1428,5,65,0,0,1428,1429,5,309,0,0,1429,1430,3,570,285,0,1430,
      1431,5,310,0,0,1431,127,1,0,0,0,1432,1433,5,66,0,0,1433,1434,5,309,
      0,0,1434,1435,5,310,0,0,1435,129,1,0,0,0,1436,1437,5,68,0,0,1437,1438,
      5,309,0,0,1438,1439,3,570,285,0,1439,1440,5,310,0,0,1440,131,1,0,0,
      0,1441,1442,5,73,0,0,1442,1443,5,309,0,0,1443,1444,5,310,0,0,1444,
      133,1,0,0,0,1445,1446,5,83,0,0,1446,1447,5,309,0,0,1447,1448,5,310,
      0,0,1448,135,1,0,0,0,1449,1450,5,82,0,0,1450,1451,5,309,0,0,1451,1452,
      3,568,284,0,1452,1453,5,310,0,0,1453,137,1,0,0,0,1454,1455,5,84,0,
      0,1455,1456,5,309,0,0,1456,1468,5,310,0,0,1457,1458,5,84,0,0,1458,
      1459,5,309,0,0,1459,1460,3,366,183,0,1460,1461,5,310,0,0,1461,1468,
      1,0,0,0,1462,1463,5,84,0,0,1463,1464,5,309,0,0,1464,1465,3,52,26,0,
      1465,1466,5,310,0,0,1466,1468,1,0,0,0,1467,1454,1,0,0,0,1467,1457,
      1,0,0,0,1467,1462,1,0,0,0,1468,139,1,0,0,0,1469,1470,5,88,0,0,1470,
      1471,5,309,0,0,1471,1478,5,310,0,0,1472,1473,5,88,0,0,1473,1474,5,
      309,0,0,1474,1475,3,578,289,0,1475,1476,5,310,0,0,1476,1478,1,0,0,
      0,1477,1469,1,0,0,0,1477,1472,1,0,0,0,1478,141,1,0,0,0,1479,1480,5,
      90,0,0,1480,1481,5,309,0,0,1481,1482,3,366,183,0,1482,1483,5,310,0,
      0,1483,1490,1,0,0,0,1484,1485,5,90,0,0,1485,1486,5,309,0,0,1486,1487,
      3,52,26,0,1487,1488,5,310,0,0,1488,1490,1,0,0,0,1489,1479,1,0,0,0,
      1489,1484,1,0,0,0,1490,143,1,0,0,0,1491,1492,5,92,0,0,1492,1493,5,
      309,0,0,1493,1494,3,52,26,0,1494,1495,5,310,0,0,1495,145,1,0,0,0,1496,
      1497,5,95,0,0,1497,1498,5,309,0,0,1498,1507,5,310,0,0,1499,1500,5,
      95,0,0,1500,1501,5,309,0,0,1501,1502,3,570,285,0,1502,1503,5,316,0,
      0,1503,1504,3,376,188,0,1504,1505,5,310,0,0,1505,1507,1,0,0,0,1506,
      1496,1,0,0,0,1506,1499,1,0,0,0,1507,147,1,0,0,0,1508,1509,5,96,0,0,
      1509,1510,5,309,0,0,1510,1511,3,578,289,0,1511,1512,5,310,0,0,1512,
      149,1,0,0,0,1513,1514,5,97,0,0,1514,1515,5,309,0,0,1515,1516,3,578,
      289,0,1516,1517,5,310,0,0,1517,1524,1,0,0,0,1518,1519,5,97,0,0,1519,
      1520,5,309,0,0,1520,1521,3,52,26,0,1521,1522,5,310,0,0,1522,1524,1,
      0,0,0,1523,1513,1,0,0,0,1523,1518,1,0,0,0,1524,151,1,0,0,0,1525,1526,
      5,105,0,0,1526,1527,5,309,0,0,1527,1534,5,310,0,0,1528,1529,5,105,
      0,0,1529,1530,5,309,0,0,1530,1531,3,578,289,0,1531,1532,5,310,0,0,
      1532,1534,1,0,0,0,1533,1525,1,0,0,0,1533,1528,1,0,0,0,1534,153,1,0,
      0,0,1535,1536,5,104,0,0,1536,1537,5,309,0,0,1537,1544,5,310,0,0,1538,
      1539,5,104,0,0,1539,1540,5,309,0,0,1540,1541,3,578,289,0,1541,1542,
      5,310,0,0,1542,1544,1,0,0,0,1543,1535,1,0,0,0,1543,1538,1,0,0,0,1544,
      155,1,0,0,0,1545,1546,5,109,0,0,1546,1547,5,309,0,0,1547,1548,3,580,
      290,0,1548,1549,5,310,0,0,1549,1597,1,0,0,0,1550,1551,5,109,0,0,1551,
      1552,5,309,0,0,1552,1553,3,580,290,0,1553,1554,5,316,0,0,1554,1555,
      3,534,267,0,1555,1556,5,310,0,0,1556,1597,1,0,0,0,1557,1558,5,109,
      0,0,1558,1559,5,309,0,0,1559,1560,3,580,290,0,1560,1561,5,316,0,0,
      1561,1562,3,366,183,0,1562,1563,5,310,0,0,1563,1597,1,0,0,0,1564,1565,
      5,109,0,0,1565,1566,5,309,0,0,1566,1567,3,528,264,0,1567,1568,5,316,
      0,0,1568,1569,3,580,290,0,1569,1570,5,316,0,0,1570,1571,3,534,267,
      0,1571,1572,5,310,0,0,1572,1597,1,0,0,0,1573,1574,5,109,0,0,1574,1575,
      5,309,0,0,1575,1576,3,528,264,0,1576,1577,5,316,0,0,1577,1578,3,580,
      290,0,1578,1579,5,316,0,0,1579,1580,3,366,183,0,1580,1581,5,310,0,
      0,1581,1597,1,0,0,0,1582,1583,5,109,0,0,1583,1584,5,309,0,0,1584,1585,
      3,336,168,0,1585,1586,5,316,0,0,1586,1587,3,534,267,0,1587,1588,5,
      310,0,0,1588,1597,1,0,0,0,1589,1590,5,109,0,0,1590,1591,5,309,0,0,
      1591,1592,3,336,168,0,1592,1593,5,316,0,0,1593,1594,3,366,183,0,1594,
      1595,5,310,0,0,1595,1597,1,0,0,0,1596,1545,1,0,0,0,1596,1550,1,0,0,
      0,1596,1557,1,0,0,0,1596,1564,1,0,0,0,1596,1573,1,0,0,0,1596,1582,
      1,0,0,0,1596,1589,1,0,0,0,1597,157,1,0,0,0,1598,1599,5,110,0,0,1599,
      1600,5,309,0,0,1600,1603,3,534,267,0,1601,1602,5,316,0,0,1602,1604,
      3,536,268,0,1603,1601,1,0,0,0,1603,1604,1,0,0,0,1604,1605,1,0,0,0,
      1605,1606,5,310,0,0,1606,1613,1,0,0,0,1607,1608,5,110,0,0,1608,1609,
      5,309,0,0,1609,1610,3,366,183,0,1610,1611,5,310,0,0,1611,1613,1,0,
      0,0,1612,1598,1,0,0,0,1612,1607,1,0,0,0,1613,159,1,0,0,0,1614,1615,
      5,111,0,0,1615,1616,5,309,0,0,1616,1617,3,366,183,0,1617,1618,5,310,
      0,0,1618,1629,1,0,0,0,1619,1620,5,111,0,0,1620,1621,5,309,0,0,1621,
      1624,3,580,290,0,1622,1623,5,316,0,0,1623,1625,3,568,284,0,1624,1622,
      1,0,0,0,1624,1625,1,0,0,0,1625,1626,1,0,0,0,1626,1627,5,310,0,0,1627,
      1629,1,0,0,0,1628,1614,1,0,0,0,1628,1619,1,0,0,0,1629,161,1,0,0,0,
      1630,1631,5,112,0,0,1631,1632,5,309,0,0,1632,1633,3,366,183,0,1633,
      1634,5,310,0,0,1634,1645,1,0,0,0,1635,1636,5,112,0,0,1636,1637,5,309,
      0,0,1637,1640,3,528,264,0,1638,1639,5,316,0,0,1639,1641,3,530,265,
      0,1640,1638,1,0,0,0,1640,1641,1,0,0,0,1641,1642,1,0,0,0,1642,1643,
      5,310,0,0,1643,1645,1,0,0,0,1644,1630,1,0,0,0,1644,1635,1,0,0,0,1645,
      163,1,0,0,0,1646,1647,5,114,0,0,1647,1648,5,309,0,0,1648,1649,3,580,
      290,0,1649,1650,5,310,0,0,1650,165,1,0,0,0,1651,1652,5,115,0,0,1652,
      1653,5,309,0,0,1653,1656,3,534,267,0,1654,1655,5,316,0,0,1655,1657,
      3,536,268,0,1656,1654,1,0,0,0,1656,1657,1,0,0,0,1657,1658,1,0,0,0,
      1658,1659,5,310,0,0,1659,1666,1,0,0,0,1660,1661,5,115,0,0,1661,1662,
      5,309,0,0,1662,1663,3,366,183,0,1663,1664,5,310,0,0,1664,1666,1,0,
      0,0,1665,1651,1,0,0,0,1665,1660,1,0,0,0,1666,167,1,0,0,0,1667,1668,
      5,117,0,0,1668,1669,5,309,0,0,1669,1670,5,310,0,0,1670,169,1,0,0,0,
      1671,1672,5,118,0,0,1672,1673,5,309,0,0,1673,1674,5,310,0,0,1674,171,
      1,0,0,0,1675,1676,5,120,0,0,1676,1677,5,309,0,0,1677,1678,3,530,265,
      0,1678,1679,5,310,0,0,1679,173,1,0,0,0,1680,1681,5,122,0,0,1681,1682,
      5,309,0,0,1682,1683,3,530,265,0,1683,1684,5,310,0,0,1684,175,1,0,0,
      0,1685,1686,5,132,0,0,1686,1687,5,309,0,0,1687,1688,3,570,285,0,1688,
      1689,5,310,0,0,1689,177,1,0,0,0,1690,1691,5,133,0,0,1691,1692,5,309,
      0,0,1692,1693,5,310,0,0,1693,179,1,0,0,0,1694,1695,5,126,0,0,1695,
      1696,5,309,0,0,1696,1697,5,310,0,0,1697,181,1,0,0,0,1698,1699,5,128,
      0,0,1699,1700,5,309,0,0,1700,1701,3,558,279,0,1701,1702,5,310,0,0,
      1702,183,1,0,0,0,1703,1704,5,136,0,0,1704,1705,5,309,0,0,1705,1706,
      3,534,267,0,1706,1707,5,310,0,0,1707,1714,1,0,0,0,1708,1709,5,136,
      0,0,1709,1710,5,309,0,0,1710,1711,3,366,183,0,1711,1712,5,310,0,0,
      1712,1714,1,0,0,0,1713,1703,1,0,0,0,1713,1708,1,0,0,0,1714,185,1,0,
      0,0,1715,1716,5,138,0,0,1716,1717,5,309,0,0,1717,1718,5,310,0,0,1718,
      187,1,0,0,0,1719,1720,5,141,0,0,1720,1721,5,309,0,0,1721,1722,5,310,
      0,0,1722,189,1,0,0,0,1723,1724,5,143,0,0,1724,1725,5,309,0,0,1725,
      1732,5,310,0,0,1726,1727,5,143,0,0,1727,1728,5,309,0,0,1728,1729,3,
      332,166,0,1729,1730,5,310,0,0,1730,1732,1,0,0,0,1731,1723,1,0,0,0,
      1731,1726,1,0,0,0,1732,191,1,0,0,0,1733,1734,5,144,0,0,1734,1735,5,
      309,0,0,1735,1736,3,332,166,0,1736,1737,5,316,0,0,1737,1738,3,524,
      262,0,1738,1739,5,310,0,0,1739,1746,1,0,0,0,1740,1741,5,144,0,0,1741,
      1742,5,309,0,0,1742,1743,3,524,262,0,1743,1744,5,310,0,0,1744,1746,
      1,0,0,0,1745,1733,1,0,0,0,1745,1740,1,0,0,0,1746,193,1,0,0,0,1747,
      1748,5,147,0,0,1748,1749,5,309,0,0,1749,1750,3,52,26,0,1750,1751,5,
      310,0,0,1751,195,1,0,0,0,1752,1753,5,150,0,0,1753,1754,5,309,0,0,1754,
      1761,5,310,0,0,1755,1756,5,150,0,0,1756,1757,5,309,0,0,1757,1758,3,
      578,289,0,1758,1759,5,310,0,0,1759,1761,1,0,0,0,1760,1752,1,0,0,0,
      1760,1755,1,0,0,0,1761,197,1,0,0,0,1762,1763,5,153,0,0,1763,1764,5,
      309,0,0,1764,1771,5,310,0,0,1765,1766,5,153,0,0,1766,1767,5,309,0,
      0,1767,1768,3,332,166,0,1768,1769,5,310,0,0,1769,1771,1,0,0,0,1770,
      1762,1,0,0,0,1770,1765,1,0,0,0,1771,199,1,0,0,0,1772,1773,5,154,0,
      0,1773,1774,5,309,0,0,1774,1775,3,52,26,0,1775,1776,5,310,0,0,1776,
      201,1,0,0,0,1777,1778,5,156,0,0,1778,1779,5,309,0,0,1779,1780,3,552,
      276,0,1780,1781,5,310,0,0,1781,203,1,0,0,0,1782,1783,5,157,0,0,1783,
      1784,5,309,0,0,1784,1785,3,578,289,0,1785,1786,5,310,0,0,1786,205,
      1,0,0,0,1787,1788,5,158,0,0,1788,1789,5,309,0,0,1789,1796,5,310,0,
      0,1790,1791,5,158,0,0,1791,1792,5,309,0,0,1792,1793,3,332,166,0,1793,
      1794,5,310,0,0,1794,1796,1,0,0,0,1795,1787,1,0,0,0,1795,1790,1,0,0,
      0,1796,207,1,0,0,0,1797,1798,5,160,0,0,1798,1799,5,309,0,0,1799,1806,
      5,310,0,0,1800,1801,5,160,0,0,1801,1802,5,309,0,0,1802,1803,3,332,
      166,0,1803,1804,5,310,0,0,1804,1806,1,0,0,0,1805,1797,1,0,0,0,1805,
      1800,1,0,0,0,1806,209,1,0,0,0,1807,1808,5,162,0,0,1808,1809,5,309,
      0,0,1809,1810,3,570,285,0,1810,1811,5,310,0,0,1811,211,1,0,0,0,1812,
      1813,5,164,0,0,1813,1814,5,309,0,0,1814,1826,5,310,0,0,1815,1816,5,
      164,0,0,1816,1817,5,309,0,0,1817,1818,3,540,270,0,1818,1819,5,310,
      0,0,1819,1826,1,0,0,0,1820,1821,5,164,0,0,1821,1822,5,309,0,0,1822,
      1823,3,52,26,0,1823,1824,5,310,0,0,1824,1826,1,0,0,0,1825,1812,1,0,
      0,0,1825,1815,1,0,0,0,1825,1820,1,0,0,0,1826,213,1,0,0,0,1827,1828,
      5,163,0,0,1828,1829,5,309,0,0,1829,1841,5,310,0,0,1830,1831,5,163,
      0,0,1831,1832,5,309,0,0,1832,1833,3,540,270,0,1833,1834,5,310,0,0,
      1834,1841,1,0,0,0,1835,1836,5,163,0,0,1836,1837,5,309,0,0,1837,1838,
      3,52,26,0,1838,1839,5,310,0,0,1839,1841,1,0,0,0,1840,1827,1,0,0,0,
      1840,1830,1,0,0,0,1840,1835,1,0,0,0,1841,215,1,0,0,0,1842,1843,5,165,
      0,0,1843,1844,5,309,0,0,1844,1851,5,310,0,0,1845,1846,5,165,0,0,1846,
      1847,5,309,0,0,1847,1848,3,332,166,0,1848,1849,5,310,0,0,1849,1851,
      1,0,0,0,1850,1842,1,0,0,0,1850,1845,1,0,0,0,1851,217,1,0,0,0,1852,
      1853,5,174,0,0,1853,1854,5,309,0,0,1854,1855,3,366,183,0,1855,1856,
      5,310,0,0,1856,219,1,0,0,0,1857,1858,5,179,0,0,1858,1859,5,309,0,0,
      1859,1860,3,52,26,0,1860,1861,5,310,0,0,1861,221,1,0,0,0,1862,1863,
      5,190,0,0,1863,1864,5,309,0,0,1864,1865,3,366,183,0,1865,1866,5,316,
      0,0,1866,1867,3,52,26,0,1867,1868,5,310,0,0,1868,1905,1,0,0,0,1869,
      1870,5,190,0,0,1870,1871,5,309,0,0,1871,1872,3,342,171,0,1872,1873,
      5,316,0,0,1873,1874,3,540,270,0,1874,1875,5,310,0,0,1875,1905,1,0,
      0,0,1876,1877,5,190,0,0,1877,1878,5,309,0,0,1878,1879,3,342,171,0,
      1879,1880,5,316,0,0,1880,1881,3,540,270,0,1881,1882,5,316,0,0,1882,
      1883,3,352,176,0,1883,1884,5,310,0,0,1884,1905,1,0,0,0,1885,1886,5,
      190,0,0,1886,1887,5,309,0,0,1887,1888,3,342,171,0,1888,1889,5,316,
      0,0,1889,1890,3,52,26,0,1890,1891,5,310,0,0,1891,1905,1,0,0,0,1892,
      1893,5,190,0,0,1893,1894,5,309,0,0,1894,1895,3,534,267,0,1895,1896,
      5,316,0,0,1896,1897,3,52,26,0,1897,1898,5,310,0,0,1898,1905,1,0,0,
      0,1899,1900,5,190,0,0,1900,1901,5,309,0,0,1901,1902,3,52,26,0,1902,
      1903,5,310,0,0,1903,1905,1,0,0,0,1904,1862,1,0,0,0,1904,1869,1,0,0,
      0,1904,1876,1,0,0,0,1904,1885,1,0,0,0,1904,1892,1,0,0,0,1904,1899,
      1,0,0,0,1905,223,1,0,0,0,1906,1907,5,191,0,0,1907,1908,5,309,0,0,1908,
      1909,3,52,26,0,1909,1910,5,310,0,0,1910,225,1,0,0,0,1911,1912,5,194,
      0,0,1912,1913,5,309,0,0,1913,1914,3,552,276,0,1914,1915,5,310,0,0,
      1915,227,1,0,0,0,1916,1917,5,193,0,0,1917,1918,5,309,0,0,1918,1925,
      5,310,0,0,1919,1920,5,193,0,0,1920,1921,5,309,0,0,1921,1922,3,332,
      166,0,1922,1923,5,310,0,0,1923,1925,1,0,0,0,1924,1916,1,0,0,0,1924,
      1919,1,0,0,0,1925,229,1,0,0,0,1926,1927,5,195,0,0,1927,1928,5,309,
      0,0,1928,1929,5,310,0,0,1929,231,1,0,0,0,1930,1931,5,197,0,0,1931,
      1932,5,309,0,0,1932,1933,3,530,265,0,1933,1934,5,310,0,0,1934,233,
      1,0,0,0,1935,1936,5,198,0,0,1936,1937,5,309,0,0,1937,1938,3,530,265,
      0,1938,1939,5,310,0,0,1939,235,1,0,0,0,1940,1941,5,200,0,0,1941,1942,
      5,309,0,0,1942,1943,5,310,0,0,1943,237,1,0,0,0,1944,1945,5,203,0,0,
      1945,1946,5,309,0,0,1946,1953,5,310,0,0,1947,1948,5,203,0,0,1948,1949,
      5,309,0,0,1949,1950,3,586,293,0,1950,1951,5,310,0,0,1951,1953,1,0,
      0,0,1952,1944,1,0,0,0,1952,1947,1,0,0,0,1953,239,1,0,0,0,1954,1955,
      5,204,0,0,1955,1956,5,309,0,0,1956,1957,5,310,0,0,1957,241,1,0,0,0,
      1958,1959,5,207,0,0,1959,1960,5,309,0,0,1960,1961,5,310,0,0,1961,243,
      1,0,0,0,1962,1963,5,217,0,0,1963,1964,5,309,0,0,1964,1965,3,570,285,
      0,1965,1966,5,310,0,0,1966,245,1,0,0,0,1967,1968,5,210,0,0,1968,1969,
      5,309,0,0,1969,1976,5,310,0,0,1970,1971,5,210,0,0,1971,1972,5,309,
      0,0,1972,1973,3,578,289,0,1973,1974,5,310,0,0,1974,1976,1,0,0,0,1975,
      1967,1,0,0,0,1975,1970,1,0,0,0,1976,247,1,0,0,0,1977,1978,5,211,0,
      0,1978,1979,5,309,0,0,1979,1982,3,578,289,0,1980,1981,5,316,0,0,1981,
      1983,3,568,284,0,1982,1980,1,0,0,0,1982,1983,1,0,0,0,1983,1984,1,0,
      0,0,1984,1985,5,310,0,0,1985,249,1,0,0,0,1986,1987,5,212,0,0,1987,
      1988,5,309,0,0,1988,1989,3,568,284,0,1989,1990,5,310,0,0,1990,251,
      1,0,0,0,1991,1992,5,215,0,0,1992,1993,5,309,0,0,1993,1994,3,352,176,
      0,1994,1995,5,316,0,0,1995,1996,3,570,285,0,1996,1997,5,316,0,0,1997,
      2000,3,534,267,0,1998,1999,5,316,0,0,1999,2001,3,536,268,0,2000,1998,
      1,0,0,0,2000,2001,1,0,0,0,2001,2002,1,0,0,0,2002,2003,5,310,0,0,2003,
      2028,1,0,0,0,2004,2005,5,215,0,0,2005,2006,5,309,0,0,2006,2007,3,352,
      176,0,2007,2008,5,316,0,0,2008,2009,3,540,270,0,2009,2010,5,310,0,
      0,2010,2028,1,0,0,0,2011,2012,5,215,0,0,2012,2013,5,309,0,0,2013,2014,
      3,570,285,0,2014,2015,5,316,0,0,2015,2018,3,534,267,0,2016,2017,5,
      316,0,0,2017,2019,3,536,268,0,2018,2016,1,0,0,0,2018,2019,1,0,0,0,
      2019,2020,1,0,0,0,2020,2021,5,310,0,0,2021,2028,1,0,0,0,2022,2023,
      5,215,0,0,2023,2024,5,309,0,0,2024,2025,3,540,270,0,2025,2026,5,310,
      0,0,2026,2028,1,0,0,0,2027,1991,1,0,0,0,2027,2004,1,0,0,0,2027,2011,
      1,0,0,0,2027,2022,1,0,0,0,2028,253,1,0,0,0,2029,2030,5,213,0,0,2030,
      2031,5,309,0,0,2031,2032,3,568,284,0,2032,2033,5,310,0,0,2033,255,
      1,0,0,0,2034,2035,5,218,0,0,2035,2036,5,309,0,0,2036,2037,3,332,166,
      0,2037,2038,5,316,0,0,2038,2039,3,524,262,0,2039,2040,5,316,0,0,2040,
      2041,3,524,262,0,2041,2042,5,310,0,0,2042,2051,1,0,0,0,2043,2044,5,
      218,0,0,2044,2045,5,309,0,0,2045,2046,3,524,262,0,2046,2047,5,316,
      0,0,2047,2048,3,524,262,0,2048,2049,5,310,0,0,2049,2051,1,0,0,0,2050,
      2034,1,0,0,0,2050,2043,1,0,0,0,2051,257,1,0,0,0,2052,2053,5,219,0,
      0,2053,2054,5,309,0,0,2054,2055,5,310,0,0,2055,259,1,0,0,0,2056,2057,
      5,223,0,0,2057,2058,5,309,0,0,2058,2059,3,578,289,0,2059,2060,5,316,
      0,0,2060,2061,3,52,26,0,2061,2062,5,310,0,0,2062,2069,1,0,0,0,2063,
      2064,5,223,0,0,2064,2065,5,309,0,0,2065,2066,3,52,26,0,2066,2067,5,
      310,0,0,2067,2069,1,0,0,0,2068,2056,1,0,0,0,2068,2063,1,0,0,0,2069,
      261,1,0,0,0,2070,2071,5,222,0,0,2071,2072,5,309,0,0,2072,2073,3,580,
      290,0,2073,2074,5,316,0,0,2074,2075,3,580,290,0,2075,2076,5,310,0,
      0,2076,2087,1,0,0,0,2077,2078,5,222,0,0,2078,2079,5,309,0,0,2079,2080,
      3,332,166,0,2080,2081,5,316,0,0,2081,2082,3,580,290,0,2082,2083,5,
      316,0,0,2083,2084,3,580,290,0,2084,2085,5,310,0,0,2085,2087,1,0,0,
      0,2086,2070,1,0,0,0,2086,2077,1,0,0,0,2087,263,1,0,0,0,2088,2089,5,
      224,0,0,2089,2090,5,309,0,0,2090,2091,5,310,0,0,2091,265,1,0,0,0,2092,
      2093,5,226,0,0,2093,2094,5,309,0,0,2094,2101,5,310,0,0,2095,2096,5,
      226,0,0,2096,2097,5,309,0,0,2097,2098,3,332,166,0,2098,2099,5,310,
      0,0,2099,2101,1,0,0,0,2100,2092,1,0,0,0,2100,2095,1,0,0,0,2101,267,
      1,0,0,0,2102,2103,5,227,0,0,2103,2104,5,309,0,0,2104,2105,3,376,188,
      0,2105,2106,5,310,0,0,2106,2111,1,0,0,0,2107,2108,5,227,0,0,2108,2109,
      5,309,0,0,2109,2111,5,310,0,0,2110,2102,1,0,0,0,2110,2107,1,0,0,0,
      2111,269,1,0,0,0,2112,2113,5,228,0,0,2113,2114,5,309,0,0,2114,2115,
      3,332,166,0,2115,2116,5,316,0,0,2116,2117,3,582,291,0,2117,2118,5,
      310,0,0,2118,2125,1,0,0,0,2119,2120,5,228,0,0,2120,2121,5,309,0,0,
      2121,2122,3,582,291,0,2122,2123,5,310,0,0,2123,2125,1,0,0,0,2124,2112,
      1,0,0,0,2124,2119,1,0,0,0,2125,271,1,0,0,0,2126,2127,5,231,0,0,2127,
      2128,5,309,0,0,2128,2129,3,354,177,0,2129,2130,5,310,0,0,2130,2180,
      1,0,0,0,2131,2132,5,231,0,0,2132,2133,5,309,0,0,2133,2134,3,356,178,
      0,2134,2135,5,316,0,0,2135,2136,3,578,289,0,2136,2137,5,310,0,0,2137,
      2180,1,0,0,0,2138,2139,5,231,0,0,2139,2140,5,309,0,0,2140,2141,3,356,
      178,0,2141,2142,5,316,0,0,2142,2143,3,578,289,0,2143,2144,5,316,0,
      0,2144,2147,3,578,289,0,2145,2146,5,316,0,0,2146,2148,3,568,284,0,
      2147,2145,1,0,0,0,2147,2148,1,0,0,0,2148,2149,1,0,0,0,2149,2150,5,
      310,0,0,2150,2180,1,0,0,0,2151,2152,5,231,0,0,2152,2153,5,309,0,0,
      2153,2154,3,356,178,0,2154,2155,5,316,0,0,2155,2156,3,52,26,0,2156,
      2157,5,310,0,0,2157,2180,1,0,0,0,2158,2159,5,231,0,0,2159,2160,5,309,
      0,0,2160,2161,3,578,289,0,2161,2162,5,310,0,0,2162,2180,1,0,0,0,2163,
      2164,5,231,0,0,2164,2165,5,309,0,0,2165,2166,3,578,289,0,2166,2167,
      5,316,0,0,2167,2170,3,578,289,0,2168,2169,5,316,0,0,2169,2171,3,568,
      284,0,2170,2168,1,0,0,0,2170,2171,1,0,0,0,2171,2172,1,0,0,0,2172,2173,
      5,310,0,0,2173,2180,1,0,0,0,2174,2175,5,231,0,0,2175,2176,5,309,0,
      0,2176,2177,3,52,26,0,2177,2178,5,310,0,0,2178,2180,1,0,0,0,2179,2126,
      1,0,0,0,2179,2131,1,0,0,0,2179,2138,1,0,0,0,2179,2151,1,0,0,0,2179,
      2158,1,0,0,0,2179,2163,1,0,0,0,2179,2174,1,0,0,0,2180,273,1,0,0,0,
      2181,2182,5,235,0,0,2182,2183,5,309,0,0,2183,2184,5,310,0,0,2184,275,
      1,0,0,0,2185,2186,5,239,0,0,2186,2187,5,309,0,0,2187,2188,3,52,26,
      0,2188,2189,5,310,0,0,2189,277,1,0,0,0,2190,2191,5,240,0,0,2191,2192,
      5,309,0,0,2192,2193,5,310,0,0,2193,279,1,0,0,0,2194,2195,5,242,0,0,
      2195,2196,5,309,0,0,2196,2197,3,332,166,0,2197,2198,5,316,0,0,2198,
      2199,3,524,262,0,2199,2200,5,310,0,0,2200,2207,1,0,0,0,2201,2202,5,
      242,0,0,2202,2203,5,309,0,0,2203,2204,3,524,262,0,2204,2205,5,310,
      0,0,2205,2207,1,0,0,0,2206,2194,1,0,0,0,2206,2201,1,0,0,0,2207,281,
      1,0,0,0,2208,2209,5,243,0,0,2209,2210,5,309,0,0,2210,2211,3,580,290,
      0,2211,2212,5,310,0,0,2212,2221,1,0,0,0,2213,2214,5,243,0,0,2214,2215,
      5,309,0,0,2215,2216,3,332,166,0,2216,2217,5,316,0,0,2217,2218,3,580,
      290,0,2218,2219,5,310,0,0,2219,2221,1,0,0,0,2220,2208,1,0,0,0,2220,
      2213,1,0,0,0,2221,283,1,0,0,0,2222,2223,5,247,0,0,2223,2224,5,309,
      0,0,2224,2225,3,578,289,0,2225,2226,5,310,0,0,2226,285,1,0,0,0,2227,
      2228,5,248,0,0,2228,2229,5,309,0,0,2229,2230,3,582,291,0,2230,2231,
      5,310,0,0,2231,2256,1,0,0,0,2232,2233,5,248,0,0,2233,2234,5,309,0,
      0,2234,2235,3,332,166,0,2235,2236,5,316,0,0,2236,2237,3,582,291,0,
      2237,2238,5,310,0,0,2238,2256,1,0,0,0,2239,2240,5,248,0,0,2240,2241,
      5,309,0,0,2241,2242,3,582,291,0,2242,2243,5,316,0,0,2243,2244,3,582,
      291,0,2244,2245,5,310,0,0,2245,2256,1,0,0,0,2246,2247,5,248,0,0,2247,
      2248,5,309,0,0,2248,2249,3,332,166,0,2249,2250,5,316,0,0,2250,2251,
      3,582,291,0,2251,2252,5,316,0,0,2252,2253,3,582,291,0,2253,2254,5,
      310,0,0,2254,2256,1,0,0,0,2255,2227,1,0,0,0,2255,2232,1,0,0,0,2255,
      2239,1,0,0,0,2255,2246,1,0,0,0,2256,287,1,0,0,0,2257,2258,5,249,0,
      0,2258,2259,5,309,0,0,2259,2266,5,310,0,0,2260,2261,5,249,0,0,2261,
      2262,5,309,0,0,2262,2263,3,332,166,0,2263,2264,5,310,0,0,2264,2266,
      1,0,0,0,2265,2257,1,0,0,0,2265,2260,1,0,0,0,2266,289,1,0,0,0,2267,
      2268,5,252,0,0,2268,2269,5,309,0,0,2269,2288,5,310,0,0,2270,2271,5,
      252,0,0,2271,2272,5,309,0,0,2272,2273,3,332,166,0,2273,2274,5,310,
      0,0,2274,2288,1,0,0,0,2275,2276,5,252,0,0,2276,2277,5,309,0,0,2277,
      2278,3,332,166,0,2278,2279,5,316,0,0,2279,2280,3,524,262,0,2280,2281,
      5,310,0,0,2281,2288,1,0,0,0,2282,2283,5,252,0,0,2283,2284,5,309,0,
      0,2284,2285,3,524,262,0,2285,2286,5,310,0,0,2286,2288,1,0,0,0,2287,
      2267,1,0,0,0,2287,2270,1,0,0,0,2287,2275,1,0,0,0,2287,2282,1,0,0,0,
      2288,291,1,0,0,0,2289,2290,5,255,0,0,2290,2291,5,309,0,0,2291,2292,
      3,582,291,0,2292,2293,5,310,0,0,2293,293,1,0,0,0,2294,2295,5,256,0,
      0,2295,2296,5,309,0,0,2296,2297,3,582,291,0,2297,2298,5,310,0,0,2298,
      295,1,0,0,0,2299,2300,5,257,0,0,2300,2301,5,309,0,0,2301,2304,3,346,
      173,0,2302,2303,5,316,0,0,2303,2305,3,530,265,0,2304,2302,1,0,0,0,
      2304,2305,1,0,0,0,2305,2306,1,0,0,0,2306,2307,5,310,0,0,2307,2319,
      1,0,0,0,2308,2309,5,257,0,0,2309,2310,5,309,0,0,2310,2311,3,578,289,
      0,2311,2312,5,310,0,0,2312,2319,1,0,0,0,2313,2314,5,257,0,0,2314,2315,
      5,309,0,0,2315,2316,3,52,26,0,2316,2317,5,310,0,0,2317,2319,1,0,0,
      0,2318,2299,1,0,0,0,2318,2308,1,0,0,0,2318,2313,1,0,0,0,2319,297,1,
      0,0,0,2320,2321,5,265,0,0,2321,2322,5,309,0,0,2322,2325,3,346,173,
      0,2323,2324,5,316,0,0,2324,2326,3,530,265,0,2325,2323,1,0,0,0,2325,
      2326,1,0,0,0,2326,2327,1,0,0,0,2327,2328,5,310,0,0,2328,299,1,0,0,
      0,2329,2330,5,261,0,0,2330,2331,5,309,0,0,2331,2338,5,310,0,0,2332,
      2333,5,261,0,0,2333,2334,5,309,0,0,2334,2335,3,332,166,0,2335,2336,
      5,310,0,0,2336,2338,1,0,0,0,2337,2329,1,0,0,0,2337,2332,1,0,0,0,2338,
      301,1,0,0,0,2339,2340,5,264,0,0,2340,2341,5,309,0,0,2341,2348,5,310,
      0,0,2342,2343,5,264,0,0,2343,2344,5,309,0,0,2344,2345,3,332,166,0,
      2345,2346,5,310,0,0,2346,2348,1,0,0,0,2347,2339,1,0,0,0,2347,2342,
      1,0,0,0,2348,303,1,0,0,0,2349,2350,5,266,0,0,2350,2351,5,309,0,0,2351,
      2352,3,346,173,0,2352,2353,5,310,0,0,2353,305,1,0,0,0,2354,2355,5,
      267,0,0,2355,2356,5,309,0,0,2356,2363,5,310,0,0,2357,2358,5,267,0,
      0,2358,2359,5,309,0,0,2359,2360,3,578,289,0,2360,2361,5,310,0,0,2361,
      2363,1,0,0,0,2362,2354,1,0,0,0,2362,2357,1,0,0,0,2363,307,1,0,0,0,
      2364,2365,5,269,0,0,2365,2366,5,309,0,0,2366,2373,5,310,0,0,2367,2368,
      5,269,0,0,2368,2369,5,309,0,0,2369,2370,3,332,166,0,2370,2371,5,310,
      0,0,2371,2373,1,0,0,0,2372,2364,1,0,0,0,2372,2367,1,0,0,0,2373,309,
      1,0,0,0,2374,2375,5,274,0,0,2375,2376,5,309,0,0,2376,2377,5,310,0,
      0,2377,311,1,0,0,0,2378,2379,5,275,0,0,2379,2380,5,309,0,0,2380,2381,
      3,552,276,0,2381,2382,5,310,0,0,2382,313,1,0,0,0,2383,2384,5,277,0,
      0,2384,2385,5,309,0,0,2385,2386,3,366,183,0,2386,2387,5,310,0,0,2387,
      2394,1,0,0,0,2388,2389,5,277,0,0,2389,2390,5,309,0,0,2390,2391,3,52,
      26,0,2391,2392,5,310,0,0,2392,2394,1,0,0,0,2393,2383,1,0,0,0,2393,
      2388,1,0,0,0,2394,315,1,0,0,0,2395,2396,5,283,0,0,2396,2397,5,309,
      0,0,2397,2398,5,310,0,0,2398,317,1,0,0,0,2399,2400,5,281,0,0,2400,
      2401,5,309,0,0,2401,2402,3,568,284,0,2402,2403,5,310,0,0,2403,2414,
      1,0,0,0,2404,2405,5,281,0,0,2405,2406,5,309,0,0,2406,2409,3,588,294,
      0,2407,2408,5,316,0,0,2408,2410,3,568,284,0,2409,2407,1,0,0,0,2409,
      2410,1,0,0,0,2410,2411,1,0,0,0,2411,2412,5,310,0,0,2412,2414,1,0,0,
      0,2413,2399,1,0,0,0,2413,2404,1,0,0,0,2414,319,1,0,0,0,2415,2416,5,
      282,0,0,2416,2417,5,309,0,0,2417,2418,3,568,284,0,2418,2419,5,310,
      0,0,2419,321,1,0,0,0,2420,2421,5,288,0,0,2421,2422,5,309,0,0,2422,
      2423,3,366,183,0,2423,2424,5,310,0,0,2424,2438,1,0,0,0,2425,2426,5,
      288,0,0,2426,2427,5,309,0,0,2427,2428,3,578,289,0,2428,2429,5,316,
      0,0,2429,2430,3,366,183,0,2430,2431,5,310,0,0,2431,2438,1,0,0,0,2432,
      2433,5,288,0,0,2433,2434,5,309,0,0,2434,2435,3,52,26,0,2435,2436,5,
      310,0,0,2436,2438,1,0,0,0,2437,2420,1,0,0,0,2437,2425,1,0,0,0,2437,
      2432,1,0,0,0,2438,323,1,0,0,0,2439,2440,5,289,0,0,2440,2443,5,309,
      0,0,2441,2444,3,436,218,0,2442,2444,3,578,289,0,2443,2441,1,0,0,0,
      2443,2442,1,0,0,0,2444,2445,1,0,0,0,2445,2446,5,310,0,0,2446,2462,
      1,0,0,0,2447,2448,5,289,0,0,2448,2451,5,309,0,0,2449,2452,3,436,218,
      0,2450,2452,3,578,289,0,2451,2449,1,0,0,0,2451,2450,1,0,0,0,2452,2453,
      1,0,0,0,2453,2457,5,316,0,0,2454,2458,3,446,223,0,2455,2458,3,450,
      225,0,2456,2458,3,570,285,0,2457,2454,1,0,0,0,2457,2455,1,0,0,0,2457,
      2456,1,0,0,0,2458,2459,1,0,0,0,2459,2460,5,310,0,0,2460,2462,1,0,0,
      0,2461,2439,1,0,0,0,2461,2447,1,0,0,0,2462,325,1,0,0,0,2463,2464,5,
      299,0,0,2464,2465,5,309,0,0,2465,2466,5,310,0,0,2466,327,1,0,0,0,2467,
      2469,5,181,0,0,2468,2467,1,0,0,0,2468,2469,1,0,0,0,2469,2470,1,0,0,
      0,2470,2483,3,608,304,0,2471,2480,5,309,0,0,2472,2477,3,330,165,0,
      2473,2474,5,316,0,0,2474,2476,3,330,165,0,2475,2473,1,0,0,0,2476,2479,
      1,0,0,0,2477,2475,1,0,0,0,2477,2478,1,0,0,0,2478,2481,1,0,0,0,2479,
      2477,1,0,0,0,2480,2472,1,0,0,0,2480,2481,1,0,0,0,2481,2482,1,0,0,0,
      2482,2484,5,310,0,0,2483,2471,1,0,0,0,2483,2484,1,0,0,0,2484,329,1,
      0,0,0,2485,2488,3,612,306,0,2486,2488,3,606,303,0,2487,2485,1,0,0,
      0,2487,2486,1,0,0,0,2488,2489,1,0,0,0,2489,2490,5,318,0,0,2490,2491,
      3,534,267,0,2491,331,1,0,0,0,2492,2501,5,147,0,0,2493,2494,5,229,0,
      0,2494,2495,5,317,0,0,2495,2501,5,147,0,0,2496,2501,5,98,0,0,2497,
      2498,5,229,0,0,2498,2499,5,317,0,0,2499,2501,5,98,0,0,2500,2492,1,
      0,0,0,2500,2493,1,0,0,0,2500,2496,1,0,0,0,2500,2497,1,0,0,0,2501,333,
      1,0,0,0,2502,2507,5,182,0,0,2503,2504,5,16,0,0,2504,2505,5,317,0,0,
      2505,2507,5,182,0,0,2506,2502,1,0,0,0,2506,2503,1,0,0,0,2507,335,1,
      0,0,0,2508,2511,3,338,169,0,2509,2511,3,340,170,0,2510,2508,1,0,0,
      0,2510,2509,1,0,0,0,2511,337,1,0,0,0,2512,2513,7,0,0,0,2513,339,1,
      0,0,0,2514,2515,5,251,0,0,2515,2516,5,317,0,0,2516,2527,5,117,0,0,
      2517,2518,5,251,0,0,2518,2519,5,317,0,0,2519,2527,5,141,0,0,2520,2521,
      5,251,0,0,2521,2522,5,317,0,0,2522,2527,5,138,0,0,2523,2524,5,251,
      0,0,2524,2525,5,317,0,0,2525,2527,5,283,0,0,2526,2514,1,0,0,0,2526,
      2517,1,0,0,0,2526,2520,1,0,0,0,2526,2523,1,0,0,0,2527,341,1,0,0,0,
      2528,2545,5,187,0,0,2529,2530,5,161,0,0,2530,2531,5,317,0,0,2531,2545,
      5,187,0,0,2532,2545,5,188,0,0,2533,2534,5,161,0,0,2534,2535,5,317,
      0,0,2535,2545,5,188,0,0,2536,2545,5,200,0,0,2537,2538,5,161,0,0,2538,
      2539,5,317,0,0,2539,2545,5,200,0,0,2540,2545,5,133,0,0,2541,2542,5,
      161,0,0,2542,2543,5,317,0,0,2543,2545,5,133,0,0,2544,2528,1,0,0,0,
      2544,2529,1,0,0,0,2544,2532,1,0,0,0,2544,2533,1,0,0,0,2544,2536,1,
      0,0,0,2544,2537,1,0,0,0,2544,2540,1,0,0,0,2544,2541,1,0,0,0,2545,343,
      1,0,0,0,2546,2559,5,10,0,0,2547,2548,5,192,0,0,2548,2549,5,317,0,0,
      2549,2559,5,10,0,0,2550,2559,5,64,0,0,2551,2552,5,192,0,0,2552,2553,
      5,317,0,0,2553,2559,5,64,0,0,2554,2559,5,236,0,0,2555,2556,5,192,0,
      0,2556,2557,5,317,0,0,2557,2559,5,236,0,0,2558,2546,1,0,0,0,2558,2547,
      1,0,0,0,2558,2550,1,0,0,0,2558,2551,1,0,0,0,2558,2554,1,0,0,0,2558,
      2555,1,0,0,0,2559,345,1,0,0,0,2560,2563,3,348,174,0,2561,2563,3,350,
      175,0,2562,2560,1,0,0,0,2562,2561,1,0,0,0,2563,347,1,0,0,0,2564,2565,
      7,1,0,0,2565,349,1,0,0,0,2566,2567,5,67,0,0,2567,2568,5,317,0,0,2568,
      2582,5,121,0,0,2569,2570,5,67,0,0,2570,2571,5,317,0,0,2571,2582,5,
      97,0,0,2572,2573,5,67,0,0,2573,2574,5,317,0,0,2574,2582,5,196,0,0,
      2575,2576,5,67,0,0,2576,2577,5,317,0,0,2577,2582,5,257,0,0,2578,2579,
      5,67,0,0,2579,2580,5,317,0,0,2580,2582,5,29,0,0,2581,2566,1,0,0,0,
      2581,2569,1,0,0,0,2581,2572,1,0,0,0,2581,2575,1,0,0,0,2581,2578,1,
      0,0,0,2582,351,1,0,0,0,2583,2584,5,38,0,0,2584,2585,5,317,0,0,2585,
      2588,5,241,0,0,2586,2588,5,241,0,0,2587,2583,1,0,0,0,2587,2586,1,0,
      0,0,2588,2589,1,0,0,0,2589,2590,5,309,0,0,2590,2591,3,570,285,0,2591,
      2592,5,310,0,0,2592,2626,1,0,0,0,2593,2594,5,38,0,0,2594,2595,5,317,
      0,0,2595,2598,5,232,0,0,2596,2598,5,232,0,0,2597,2593,1,0,0,0,2597,
      2596,1,0,0,0,2598,2599,1,0,0,0,2599,2600,5,309,0,0,2600,2601,3,570,
      285,0,2601,2602,5,310,0,0,2602,2626,1,0,0,0,2603,2604,5,38,0,0,2604,
      2605,5,317,0,0,2605,2608,5,145,0,0,2606,2608,5,145,0,0,2607,2603,1,
      0,0,0,2607,2606,1,0,0,0,2608,2609,1,0,0,0,2609,2610,5,309,0,0,2610,
      2611,3,570,285,0,2611,2612,5,310,0,0,2612,2626,1,0,0,0,2613,2626,5,
      241,0,0,2614,2615,5,38,0,0,2615,2616,5,317,0,0,2616,2626,5,241,0,0,
      2617,2626,5,232,0,0,2618,2619,5,38,0,0,2619,2620,5,317,0,0,2620,2626,
      5,232,0,0,2621,2626,5,145,0,0,2622,2623,5,38,0,0,2623,2624,5,317,0,
      0,2624,2626,5,145,0,0,2625,2587,1,0,0,0,2625,2597,1,0,0,0,2625,2607,
      1,0,0,0,2625,2613,1,0,0,0,2625,2614,1,0,0,0,2625,2617,1,0,0,0,2625,
      2618,1,0,0,0,2625,2621,1,0,0,0,2625,2622,1,0,0,0,2626,353,1,0,0,0,
      2627,2636,5,139,0,0,2628,2629,5,44,0,0,2629,2630,5,317,0,0,2630,2636,
      5,139,0,0,2631,2636,5,282,0,0,2632,2633,5,44,0,0,2633,2634,5,317,0,
      0,2634,2636,5,282,0,0,2635,2627,1,0,0,0,2635,2628,1,0,0,0,2635,2631,
      1,0,0,0,2635,2632,1,0,0,0,2636,355,1,0,0,0,2637,2654,5,91,0,0,2638,
      2639,5,209,0,0,2639,2640,5,317,0,0,2640,2654,5,91,0,0,2641,2654,5,
      142,0,0,2642,2643,5,209,0,0,2643,2644,5,317,0,0,2644,2654,5,142,0,
      0,2645,2654,5,5,0,0,2646,2647,5,209,0,0,2647,2648,5,317,0,0,2648,2654,
      5,5,0,0,2649,2654,5,168,0,0,2650,2651,5,209,0,0,2651,2652,5,317,0,
      0,2652,2654,5,168,0,0,2653,2637,1,0,0,0,2653,2638,1,0,0,0,2653,2641,
      1,0,0,0,2653,2642,1,0,0,0,2653,2645,1,0,0,0,2653,2646,1,0,0,0,2653,
      2649,1,0,0,0,2653,2650,1,0,0,0,2654,357,1,0,0,0,2655,2700,5,1,0,0,
      2656,2657,5,189,0,0,2657,2658,5,317,0,0,2658,2700,5,1,0,0,2659,2700,
      5,6,0,0,2660,2661,5,189,0,0,2661,2662,5,317,0,0,2662,2700,5,6,0,0,
      2663,2700,5,14,0,0,2664,2665,5,189,0,0,2665,2666,5,317,0,0,2666,2700,
      5,14,0,0,2667,2700,5,70,0,0,2668,2669,5,189,0,0,2669,2670,5,317,0,
      0,2670,2700,5,70,0,0,2671,2700,5,158,0,0,2672,2673,5,189,0,0,2673,
      2674,5,317,0,0,2674,2700,5,158,0,0,2675,2700,5,165,0,0,2676,2677,5,
      189,0,0,2677,2678,5,317,0,0,2678,2700,5,165,0,0,2679,2700,5,167,0,
      0,2680,2681,5,189,0,0,2681,2682,5,317,0,0,2682,2700,5,167,0,0,2683,
      2700,5,169,0,0,2684,2685,5,189,0,0,2685,2686,5,317,0,0,2686,2700,5,
      169,0,0,2687,2700,5,194,0,0,2688,2689,5,189,0,0,2689,2690,5,317,0,
      0,2690,2700,5,194,0,0,2691,2700,5,249,0,0,2692,2693,5,189,0,0,2693,
      2694,5,317,0,0,2694,2700,5,249,0,0,2695,2700,5,250,0,0,2696,2697,5,
      189,0,0,2697,2698,5,317,0,0,2698,2700,5,250,0,0,2699,2655,1,0,0,0,
      2699,2656,1,0,0,0,2699,2659,1,0,0,0,2699,2660,1,0,0,0,2699,2663,1,
      0,0,0,2699,2664,1,0,0,0,2699,2667,1,0,0,0,2699,2668,1,0,0,0,2699,2671,
      1,0,0,0,2699,2672,1,0,0,0,2699,2675,1,0,0,0,2699,2676,1,0,0,0,2699,
      2679,1,0,0,0,2699,2680,1,0,0,0,2699,2683,1,0,0,0,2699,2684,1,0,0,0,
      2699,2687,1,0,0,0,2699,2688,1,0,0,0,2699,2691,1,0,0,0,2699,2692,1,
      0,0,0,2699,2695,1,0,0,0,2699,2696,1,0,0,0,2700,359,1,0,0,0,2701,2714,
      5,7,0,0,2702,2703,5,208,0,0,2703,2704,5,317,0,0,2704,2714,5,7,0,0,
      2705,2714,5,174,0,0,2706,2707,5,208,0,0,2707,2708,5,317,0,0,2708,2714,
      5,174,0,0,2709,2714,5,276,0,0,2710,2711,5,208,0,0,2711,2712,5,317,
      0,0,2712,2714,5,276,0,0,2713,2701,1,0,0,0,2713,2702,1,0,0,0,2713,2705,
      1,0,0,0,2713,2706,1,0,0,0,2713,2709,1,0,0,0,2713,2710,1,0,0,0,2714,
      361,1,0,0,0,2715,2732,5,230,0,0,2716,2717,5,74,0,0,2717,2718,5,317,
      0,0,2718,2732,5,230,0,0,2719,2732,5,166,0,0,2720,2721,5,74,0,0,2721,
      2722,5,317,0,0,2722,2732,5,166,0,0,2723,2732,5,116,0,0,2724,2725,5,
      74,0,0,2725,2726,5,317,0,0,2726,2732,5,116,0,0,2727,2732,5,56,0,0,
      2728,2729,5,74,0,0,2729,2730,5,317,0,0,2730,2732,5,56,0,0,2731,2715,
      1,0,0,0,2731,2716,1,0,0,0,2731,2719,1,0,0,0,2731,2720,1,0,0,0,2731,
      2723,1,0,0,0,2731,2724,1,0,0,0,2731,2727,1,0,0,0,2731,2728,1,0,0,0,
      2732,363,1,0,0,0,2733,2950,5,19,0,0,2734,2735,5,101,0,0,2735,2736,
      5,317,0,0,2736,2950,5,19,0,0,2737,2950,5,20,0,0,2738,2739,5,101,0,
      0,2739,2740,5,317,0,0,2740,2950,5,20,0,0,2741,2950,5,21,0,0,2742,2743,
      5,101,0,0,2743,2744,5,317,0,0,2744,2950,5,21,0,0,2745,2950,5,22,0,
      0,2746,2747,5,101,0,0,2747,2748,5,317,0,0,2748,2950,5,22,0,0,2749,
      2950,5,23,0,0,2750,2751,5,101,0,0,2751,2752,5,317,0,0,2752,2950,5,
      23,0,0,2753,2950,5,25,0,0,2754,2755,5,101,0,0,2755,2756,5,317,0,0,
      2756,2950,5,25,0,0,2757,2950,5,26,0,0,2758,2759,5,101,0,0,2759,2760,
      5,317,0,0,2760,2950,5,26,0,0,2761,2950,5,27,0,0,2762,2763,5,101,0,
      0,2763,2764,5,317,0,0,2764,2950,5,27,0,0,2765,2950,5,34,0,0,2766,2767,
      5,101,0,0,2767,2768,5,317,0,0,2768,2950,5,34,0,0,2769,2950,5,35,0,
      0,2770,2771,5,101,0,0,2771,2772,5,317,0,0,2772,2950,5,35,0,0,2773,
      2950,5,39,0,0,2774,2775,5,101,0,0,2775,2776,5,317,0,0,2776,2950,5,
      39,0,0,2777,2950,5,40,0,0,2778,2779,5,101,0,0,2779,2780,5,317,0,0,
      2780,2950,5,40,0,0,2781,2950,5,59,0,0,2782,2783,5,101,0,0,2783,2784,
      5,317,0,0,2784,2950,5,59,0,0,2785,2950,5,61,0,0,2786,2787,5,101,0,
      0,2787,2788,5,317,0,0,2788,2950,5,61,0,0,2789,2950,5,71,0,0,2790,2791,
      5,101,0,0,2791,2792,5,317,0,0,2792,2950,5,71,0,0,2793,2950,5,72,0,
      0,2794,2795,5,101,0,0,2795,2796,5,317,0,0,2796,2950,5,72,0,0,2797,
      2950,5,75,0,0,2798,2799,5,101,0,0,2799,2800,5,317,0,0,2800,2950,5,
      75,0,0,2801,2950,5,77,0,0,2802,2803,5,101,0,0,2803,2804,5,317,0,0,
      2804,2950,5,77,0,0,2805,2950,5,79,0,0,2806,2807,5,101,0,0,2807,2808,
      5,317,0,0,2808,2950,5,79,0,0,2809,2950,5,80,0,0,2810,2811,5,101,0,
      0,2811,2812,5,317,0,0,2812,2950,5,80,0,0,2813,2950,5,93,0,0,2814,2815,
      5,101,0,0,2815,2816,5,317,0,0,2816,2950,5,93,0,0,2817,2950,5,94,0,
      0,2818,2819,5,101,0,0,2819,2820,5,317,0,0,2820,2950,5,94,0,0,2821,
      2950,5,107,0,0,2822,2823,5,101,0,0,2823,2824,5,317,0,0,2824,2950,5,
      107,0,0,2825,2950,5,108,0,0,2826,2827,5,101,0,0,2827,2828,5,317,0,
      0,2828,2950,5,108,0,0,2829,2950,5,130,0,0,2830,2831,5,101,0,0,2831,
      2832,5,317,0,0,2832,2950,5,130,0,0,2833,2950,5,131,0,0,2834,2835,5,
      101,0,0,2835,2836,5,317,0,0,2836,2950,5,131,0,0,2837,2950,5,145,0,
      0,2838,2839,5,101,0,0,2839,2840,5,317,0,0,2840,2950,5,145,0,0,2841,
      2950,5,146,0,0,2842,2843,5,101,0,0,2843,2844,5,317,0,0,2844,2950,5,
      146,0,0,2845,2950,5,148,0,0,2846,2847,5,101,0,0,2847,2848,5,317,0,
      0,2848,2950,5,148,0,0,2849,2950,5,149,0,0,2850,2851,5,101,0,0,2851,
      2852,5,317,0,0,2852,2950,5,149,0,0,2853,2950,5,154,0,0,2854,2855,5,
      101,0,0,2855,2856,5,317,0,0,2856,2950,5,154,0,0,2857,2950,5,155,0,
      0,2858,2859,5,101,0,0,2859,2860,5,317,0,0,2860,2950,5,155,0,0,2861,
      2950,5,183,0,0,2862,2863,5,101,0,0,2863,2864,5,317,0,0,2864,2950,5,
      183,0,0,2865,2950,5,184,0,0,2866,2867,5,101,0,0,2867,2868,5,317,0,
      0,2868,2950,5,184,0,0,2869,2950,5,185,0,0,2870,2871,5,101,0,0,2871,
      2872,5,317,0,0,2872,2950,5,185,0,0,2873,2950,5,186,0,0,2874,2875,5,
      101,0,0,2875,2876,5,317,0,0,2876,2950,5,186,0,0,2877,2950,5,204,0,
      0,2878,2879,5,101,0,0,2879,2880,5,317,0,0,2880,2950,5,204,0,0,2881,
      2950,5,205,0,0,2882,2883,5,101,0,0,2883,2884,5,317,0,0,2884,2950,5,
      205,0,0,2885,2950,5,215,0,0,2886,2887,5,101,0,0,2887,2888,5,317,0,
      0,2888,2950,5,215,0,0,2889,2950,5,216,0,0,2890,2891,5,101,0,0,2891,
      2892,5,317,0,0,2892,2950,5,216,0,0,2893,2950,5,232,0,0,2894,2895,5,
      101,0,0,2895,2896,5,317,0,0,2896,2950,5,232,0,0,2897,2950,5,233,0,
      0,2898,2899,5,101,0,0,2899,2900,5,317,0,0,2900,2950,5,233,0,0,2901,
      2950,5,237,0,0,2902,2903,5,101,0,0,2903,2904,5,317,0,0,2904,2950,5,
      237,0,0,2905,2950,5,238,0,0,2906,2907,5,101,0,0,2907,2908,5,317,0,
      0,2908,2950,5,238,0,0,2909,2950,5,245,0,0,2910,2911,5,101,0,0,2911,
      2912,5,317,0,0,2912,2950,5,245,0,0,2913,2950,5,246,0,0,2914,2915,5,
      101,0,0,2915,2916,5,317,0,0,2916,2950,5,246,0,0,2917,2950,5,267,0,
      0,2918,2919,5,101,0,0,2919,2920,5,317,0,0,2920,2950,5,267,0,0,2921,
      2950,5,268,0,0,2922,2923,5,101,0,0,2923,2924,5,317,0,0,2924,2950,5,
      268,0,0,2925,2950,5,278,0,0,2926,2927,5,101,0,0,2927,2928,5,317,0,
      0,2928,2950,5,278,0,0,2929,2950,5,279,0,0,2930,2931,5,101,0,0,2931,
      2932,5,317,0,0,2932,2950,5,279,0,0,2933,2950,5,284,0,0,2934,2935,5,
      101,0,0,2935,2936,5,317,0,0,2936,2950,5,284,0,0,2937,2950,5,285,0,
      0,2938,2939,5,101,0,0,2939,2940,5,317,0,0,2940,2950,5,285,0,0,2941,
      2950,5,286,0,0,2942,2943,5,101,0,0,2943,2944,5,317,0,0,2944,2950,5,
      286,0,0,2945,2950,5,287,0,0,2946,2947,5,101,0,0,2947,2948,5,317,0,
      0,2948,2950,5,287,0,0,2949,2733,1,0,0,0,2949,2734,1,0,0,0,2949,2737,
      1,0,0,0,2949,2738,1,0,0,0,2949,2741,1,0,0,0,2949,2742,1,0,0,0,2949,
      2745,1,0,0,0,2949,2746,1,0,0,0,2949,2749,1,0,0,0,2949,2750,1,0,0,0,
      2949,2753,1,0,0,0,2949,2754,1,0,0,0,2949,2757,1,0,0,0,2949,2758,1,
      0,0,0,2949,2761,1,0,0,0,2949,2762,1,0,0,0,2949,2765,1,0,0,0,2949,2766,
      1,0,0,0,2949,2769,1,0,0,0,2949,2770,1,0,0,0,2949,2773,1,0,0,0,2949,
      2774,1,0,0,0,2949,2777,1,0,0,0,2949,2778,1,0,0,0,2949,2781,1,0,0,0,
      2949,2782,1,0,0,0,2949,2785,1,0,0,0,2949,2786,1,0,0,0,2949,2789,1,
      0,0,0,2949,2790,1,0,0,0,2949,2793,1,0,0,0,2949,2794,1,0,0,0,2949,2797,
      1,0,0,0,2949,2798,1,0,0,0,2949,2801,1,0,0,0,2949,2802,1,0,0,0,2949,
      2805,1,0,0,0,2949,2806,1,0,0,0,2949,2809,1,0,0,0,2949,2810,1,0,0,0,
      2949,2813,1,0,0,0,2949,2814,1,0,0,0,2949,2817,1,0,0,0,2949,2818,1,
      0,0,0,2949,2821,1,0,0,0,2949,2822,1,0,0,0,2949,2825,1,0,0,0,2949,2826,
      1,0,0,0,2949,2829,1,0,0,0,2949,2830,1,0,0,0,2949,2833,1,0,0,0,2949,
      2834,1,0,0,0,2949,2837,1,0,0,0,2949,2838,1,0,0,0,2949,2841,1,0,0,0,
      2949,2842,1,0,0,0,2949,2845,1,0,0,0,2949,2846,1,0,0,0,2949,2849,1,
      0,0,0,2949,2850,1,0,0,0,2949,2853,1,0,0,0,2949,2854,1,0,0,0,2949,2857,
      1,0,0,0,2949,2858,1,0,0,0,2949,2861,1,0,0,0,2949,2862,1,0,0,0,2949,
      2865,1,0,0,0,2949,2866,1,0,0,0,2949,2869,1,0,0,0,2949,2870,1,0,0,0,
      2949,2873,1,0,0,0,2949,2874,1,0,0,0,2949,2877,1,0,0,0,2949,2878,1,
      0,0,0,2949,2881,1,0,0,0,2949,2882,1,0,0,0,2949,2885,1,0,0,0,2949,2886,
      1,0,0,0,2949,2889,1,0,0,0,2949,2890,1,0,0,0,2949,2893,1,0,0,0,2949,
      2894,1,0,0,0,2949,2897,1,0,0,0,2949,2898,1,0,0,0,2949,2901,1,0,0,0,
      2949,2902,1,0,0,0,2949,2905,1,0,0,0,2949,2906,1,0,0,0,2949,2909,1,
      0,0,0,2949,2910,1,0,0,0,2949,2913,1,0,0,0,2949,2914,1,0,0,0,2949,2917,
      1,0,0,0,2949,2918,1,0,0,0,2949,2921,1,0,0,0,2949,2922,1,0,0,0,2949,
      2925,1,0,0,0,2949,2926,1,0,0,0,2949,2929,1,0,0,0,2949,2930,1,0,0,0,
      2949,2933,1,0,0,0,2949,2934,1,0,0,0,2949,2937,1,0,0,0,2949,2938,1,
      0,0,0,2949,2941,1,0,0,0,2949,2942,1,0,0,0,2949,2945,1,0,0,0,2949,2946,
      1,0,0,0,2950,365,1,0,0,0,2951,2952,6,183,-1,0,2952,2974,3,378,189,
      0,2953,2974,3,380,190,0,2954,2974,3,384,192,0,2955,2974,3,386,193,
      0,2956,2974,3,388,194,0,2957,2974,3,390,195,0,2958,2974,3,392,196,
      0,2959,2974,3,394,197,0,2960,2974,3,396,198,0,2961,2974,3,382,191,
      0,2962,2974,3,398,199,0,2963,2974,3,400,200,0,2964,2974,3,402,201,
      0,2965,2974,3,408,204,0,2966,2974,3,410,205,0,2967,2974,3,412,206,
      0,2968,2974,3,414,207,0,2969,2974,3,404,202,0,2970,2974,3,406,203,
      0,2971,2974,3,416,208,0,2972,2974,3,418,209,0,2973,2951,1,0,0,0,2973,
      2953,1,0,0,0,2973,2954,1,0,0,0,2973,2955,1,0,0,0,2973,2956,1,0,0,0,
      2973,2957,1,0,0,0,2973,2958,1,0,0,0,2973,2959,1,0,0,0,2973,2960,1,
      0,0,0,2973,2961,1,0,0,0,2973,2962,1,0,0,0,2973,2963,1,0,0,0,2973,2964,
      1,0,0,0,2973,2965,1,0,0,0,2973,2966,1,0,0,0,2973,2967,1,0,0,0,2973,
      2968,1,0,0,0,2973,2969,1,0,0,0,2973,2970,1,0,0,0,2973,2971,1,0,0,0,
      2973,2972,1,0,0,0,2974,2996,1,0,0,0,2975,2976,10,3,0,0,2976,2977,5,
      317,0,0,2977,2978,5,6,0,0,2978,2979,5,309,0,0,2979,2980,3,366,183,
      0,2980,2981,5,310,0,0,2981,2995,1,0,0,0,2982,2983,10,2,0,0,2983,2984,
      5,317,0,0,2984,2985,5,194,0,0,2985,2986,5,309,0,0,2986,2987,3,366,
      183,0,2987,2988,5,310,0,0,2988,2995,1,0,0,0,2989,2990,10,1,0,0,2990,
      2991,5,317,0,0,2991,2992,5,172,0,0,2992,2993,5,309,0,0,2993,2995,5,
      310,0,0,2994,2975,1,0,0,0,2994,2982,1,0,0,0,2994,2989,1,0,0,0,2995,
      2998,1,0,0,0,2996,2994,1,0,0,0,2996,2997,1,0,0,0,2997,367,1,0,0,0,
      2998,2996,1,0,0,0,2999,3008,3,420,210,0,3000,3008,3,424,212,0,3001,
      3008,3,422,211,0,3002,3008,3,426,213,0,3003,3008,3,428,214,0,3004,
      3008,3,430,215,0,3005,3008,3,432,216,0,3006,3008,3,434,217,0,3007,
      2999,1,0,0,0,3007,3000,1,0,0,0,3007,3001,1,0,0,0,3007,3002,1,0,0,0,
      3007,3003,1,0,0,0,3007,3004,1,0,0,0,3007,3005,1,0,0,0,3007,3006,1,
      0,0,0,3008,369,1,0,0,0,3009,3010,3,334,167,0,3010,371,1,0,0,0,3011,
      3012,3,344,172,0,3012,373,1,0,0,0,3013,3016,3,336,168,0,3014,3016,
      3,354,177,0,3015,3013,1,0,0,0,3015,3014,1,0,0,0,3016,375,1,0,0,0,3017,
      3018,3,358,179,0,3018,377,1,0,0,0,3019,3020,5,201,0,0,3020,3021,5,
      317,0,0,3021,3024,5,86,0,0,3022,3024,5,86,0,0,3023,3019,1,0,0,0,3023,
      3022,1,0,0,0,3024,3025,1,0,0,0,3025,3026,5,309,0,0,3026,3027,3,534,
      267,0,3027,3028,5,310,0,0,3028,379,1,0,0,0,3029,3030,5,201,0,0,3030,
      3031,5,317,0,0,3031,3034,5,180,0,0,3032,3034,5,180,0,0,3033,3029,1,
      0,0,0,3033,3032,1,0,0,0,3034,3035,1,0,0,0,3035,3036,5,309,0,0,3036,
      3037,3,534,267,0,3037,3038,5,310,0,0,3038,381,1,0,0,0,3039,3040,5,
      201,0,0,3040,3041,5,317,0,0,3041,3044,5,272,0,0,3042,3044,5,272,0,
      0,3043,3039,1,0,0,0,3043,3042,1,0,0,0,3044,3045,1,0,0,0,3045,3046,
      5,309,0,0,3046,3047,3,364,182,0,3047,3048,5,310,0,0,3048,3060,1,0,
      0,0,3049,3050,5,201,0,0,3050,3051,5,317,0,0,3051,3054,5,272,0,0,3052,
      3054,5,272,0,0,3053,3049,1,0,0,0,3053,3052,1,0,0,0,3054,3055,1,0,0,
      0,3055,3056,5,309,0,0,3056,3057,3,578,289,0,3057,3058,5,310,0,0,3058,
      3060,1,0,0,0,3059,3043,1,0,0,0,3059,3053,1,0,0,0,3060,383,1,0,0,0,
      3061,3062,5,201,0,0,3062,3063,5,317,0,0,3063,3066,5,151,0,0,3064,3066,
      5,151,0,0,3065,3061,1,0,0,0,3065,3064,1,0,0,0,3066,3067,1,0,0,0,3067,
      3068,5,309,0,0,3068,3069,3,534,267,0,3069,3070,5,310,0,0,3070,385,
      1,0,0,0,3071,3072,5,201,0,0,3072,3073,5,317,0,0,3073,3076,5,152,0,
      0,3074,3076,5,152,0,0,3075,3071,1,0,0,0,3075,3074,1,0,0,0,3076,3077,
      1,0,0,0,3077,3078,5,309,0,0,3078,3079,3,534,267,0,3079,3080,5,310,
      0,0,3080,387,1,0,0,0,3081,3082,5,201,0,0,3082,3083,5,317,0,0,3083,
      3086,5,99,0,0,3084,3086,5,99,0,0,3085,3081,1,0,0,0,3085,3084,1,0,0,
      0,3086,3087,1,0,0,0,3087,3088,5,309,0,0,3088,3089,3,534,267,0,3089,
      3090,5,310,0,0,3090,389,1,0,0,0,3091,3092,5,201,0,0,3092,3093,5,317,
      0,0,3093,3096,5,100,0,0,3094,3096,5,100,0,0,3095,3091,1,0,0,0,3095,
      3094,1,0,0,0,3096,3097,1,0,0,0,3097,3098,5,309,0,0,3098,3099,3,534,
      267,0,3099,3100,5,310,0,0,3100,391,1,0,0,0,3101,3102,5,201,0,0,3102,
      3103,5,317,0,0,3103,3106,5,129,0,0,3104,3106,5,129,0,0,3105,3101,1,
      0,0,0,3105,3104,1,0,0,0,3106,3107,1,0,0,0,3107,3108,5,309,0,0,3108,
      3109,3,534,267,0,3109,3110,5,316,0,0,3110,3111,3,534,267,0,3111,3112,
      5,310,0,0,3112,393,1,0,0,0,3113,3114,5,201,0,0,3114,3115,5,317,0,0,
      3115,3118,5,199,0,0,3116,3118,5,199,0,0,3117,3113,1,0,0,0,3117,3116,
      1,0,0,0,3118,3119,1,0,0,0,3119,3120,5,309,0,0,3120,3121,3,534,267,
      0,3121,3122,5,316,0,0,3122,3123,3,534,267,0,3123,3124,5,310,0,0,3124,
      395,1,0,0,0,3125,3126,5,201,0,0,3126,3127,5,317,0,0,3127,3130,5,18,
      0,0,3128,3130,5,18,0,0,3129,3125,1,0,0,0,3129,3128,1,0,0,0,3130,3131,
      1,0,0,0,3131,3132,5,309,0,0,3132,3133,3,534,267,0,3133,3134,5,316,
      0,0,3134,3135,3,534,267,0,3135,3136,5,310,0,0,3136,397,1,0,0,0,3137,
      3138,5,201,0,0,3138,3139,5,317,0,0,3139,3142,5,291,0,0,3140,3142,5,
      291,0,0,3141,3137,1,0,0,0,3141,3140,1,0,0,0,3142,3143,1,0,0,0,3143,
      3144,5,309,0,0,3144,3156,5,310,0,0,3145,3146,5,201,0,0,3146,3147,5,
      317,0,0,3147,3150,5,291,0,0,3148,3150,5,291,0,0,3149,3145,1,0,0,0,
      3149,3148,1,0,0,0,3150,3151,1,0,0,0,3151,3152,5,309,0,0,3152,3153,
      3,536,268,0,3153,3154,5,310,0,0,3154,3156,1,0,0,0,3155,3141,1,0,0,
      0,3155,3149,1,0,0,0,3156,399,1,0,0,0,3157,3158,5,201,0,0,3158,3159,
      5,317,0,0,3159,3162,5,293,0,0,3160,3162,5,293,0,0,3161,3157,1,0,0,
      0,3161,3160,1,0,0,0,3162,3163,1,0,0,0,3163,3164,5,309,0,0,3164,3176,
      5,310,0,0,3165,3166,5,201,0,0,3166,3167,5,317,0,0,3167,3170,5,293,
      0,0,3168,3170,5,293,0,0,3169,3165,1,0,0,0,3169,3168,1,0,0,0,3170,3171,
      1,0,0,0,3171,3172,5,309,0,0,3172,3173,3,536,268,0,3173,3174,5,310,
      0,0,3174,3176,1,0,0,0,3175,3161,1,0,0,0,3175,3169,1,0,0,0,3176,401,
      1,0,0,0,3177,3178,5,201,0,0,3178,3179,5,317,0,0,3179,3182,5,179,0,
      0,3180,3182,5,179,0,0,3181,3177,1,0,0,0,3181,3180,1,0,0,0,3182,3183,
      1,0,0,0,3183,3184,5,309,0,0,3184,3185,3,366,183,0,3185,3186,5,310,
      0,0,3186,403,1,0,0,0,3187,3188,5,254,0,0,3188,3189,5,317,0,0,3189,
      3192,5,53,0,0,3190,3192,5,53,0,0,3191,3187,1,0,0,0,3191,3190,1,0,0,
      0,3192,3193,1,0,0,0,3193,3194,5,309,0,0,3194,3195,3,526,263,0,3195,
      3196,5,310,0,0,3196,405,1,0,0,0,3197,3198,5,254,0,0,3198,3199,5,317,
      0,0,3199,3202,5,176,0,0,3200,3202,5,176,0,0,3201,3197,1,0,0,0,3201,
      3200,1,0,0,0,3202,3203,1,0,0,0,3203,3204,5,309,0,0,3204,3205,3,526,
      263,0,3205,3206,5,310,0,0,3206,407,1,0,0,0,3207,3208,5,254,0,0,3208,
      3209,5,317,0,0,3209,3212,5,244,0,0,3210,3212,5,244,0,0,3211,3207,1,
      0,0,0,3211,3210,1,0,0,0,3212,3213,1,0,0,0,3213,3214,5,309,0,0,3214,
      3215,3,526,263,0,3215,3216,5,310,0,0,3216,409,1,0,0,0,3217,3218,5,
      254,0,0,3218,3219,5,317,0,0,3219,3222,5,178,0,0,3220,3222,5,178,0,
      0,3221,3217,1,0,0,0,3221,3220,1,0,0,0,3222,3223,1,0,0,0,3223,3224,
      5,309,0,0,3224,3225,3,526,263,0,3225,3226,5,310,0,0,3226,411,1,0,0,
      0,3227,3228,5,254,0,0,3228,3229,5,317,0,0,3229,3232,5,85,0,0,3230,
      3232,5,85,0,0,3231,3227,1,0,0,0,3231,3230,1,0,0,0,3232,3233,1,0,0,
      0,3233,3234,5,309,0,0,3234,3235,3,526,263,0,3235,3236,5,310,0,0,3236,
      413,1,0,0,0,3237,3238,5,254,0,0,3238,3239,5,317,0,0,3239,3242,5,177,
      0,0,3240,3242,5,177,0,0,3241,3237,1,0,0,0,3241,3240,1,0,0,0,3242,3243,
      1,0,0,0,3243,3244,5,309,0,0,3244,3245,3,526,263,0,3245,3246,5,310,
      0,0,3246,415,1,0,0,0,3247,3248,5,254,0,0,3248,3249,5,317,0,0,3249,
      3252,5,221,0,0,3250,3252,5,221,0,0,3251,3247,1,0,0,0,3251,3250,1,0,
      0,0,3252,3253,1,0,0,0,3253,3254,5,309,0,0,3254,3255,3,526,263,0,3255,
      3256,5,310,0,0,3256,417,1,0,0,0,3257,3258,5,254,0,0,3258,3259,5,317,
      0,0,3259,3262,5,175,0,0,3260,3262,5,175,0,0,3261,3257,1,0,0,0,3261,
      3260,1,0,0,0,3262,3263,1,0,0,0,3263,3264,5,309,0,0,3264,3265,3,526,
      263,0,3265,3266,5,310,0,0,3266,419,1,0,0,0,3267,3268,5,87,0,0,3268,
      3269,5,309,0,0,3269,3270,5,310,0,0,3270,421,1,0,0,0,3271,3272,5,113,
      0,0,3272,3273,5,309,0,0,3273,3274,5,310,0,0,3274,423,1,0,0,0,3275,
      3276,5,137,0,0,3276,3277,5,309,0,0,3277,3278,5,310,0,0,3278,425,1,
      0,0,0,3279,3280,5,271,0,0,3280,3281,5,309,0,0,3281,3282,5,310,0,0,
      3282,427,1,0,0,0,3283,3284,5,173,0,0,3284,3285,5,309,0,0,3285,3292,
      5,310,0,0,3286,3287,5,173,0,0,3287,3288,5,309,0,0,3288,3289,3,582,
      291,0,3289,3290,5,310,0,0,3290,3292,1,0,0,0,3291,3283,1,0,0,0,3291,
      3286,1,0,0,0,3292,429,1,0,0,0,3293,3294,5,260,0,0,3294,3295,5,309,
      0,0,3295,3296,5,310,0,0,3296,431,1,0,0,0,3297,3298,5,262,0,0,3298,
      3299,5,309,0,0,3299,3300,5,310,0,0,3300,433,1,0,0,0,3301,3302,5,258,
      0,0,3302,3303,5,309,0,0,3303,3304,5,310,0,0,3304,435,1,0,0,0,3305,
      3313,3,444,222,0,3306,3313,3,438,219,0,3307,3313,3,440,220,0,3308,
      3313,3,442,221,0,3309,3313,3,448,224,0,3310,3313,3,480,240,0,3311,
      3313,3,494,247,0,3312,3305,1,0,0,0,3312,3306,1,0,0,0,3312,3307,1,0,
      0,0,3312,3308,1,0,0,0,3312,3309,1,0,0,0,3312,3310,1,0,0,0,3312,3311,
      1,0,0,0,3313,437,1,0,0,0,3314,3318,3,452,226,0,3315,3318,3,454,227,
      0,3316,3318,3,456,228,0,3317,3314,1,0,0,0,3317,3315,1,0,0,0,3317,3316,
      1,0,0,0,3318,439,1,0,0,0,3319,3323,3,458,229,0,3320,3323,3,460,230,
      0,3321,3323,3,462,231,0,3322,3319,1,0,0,0,3322,3320,1,0,0,0,3322,3321,
      1,0,0,0,3323,441,1,0,0,0,3324,3328,3,464,232,0,3325,3328,3,466,233,
      0,3326,3328,3,468,234,0,3327,3324,1,0,0,0,3327,3325,1,0,0,0,3327,3326,
      1,0,0,0,3328,443,1,0,0,0,3329,3335,3,470,235,0,3330,3335,3,472,236,
      0,3331,3335,3,474,237,0,3332,3335,3,476,238,0,3333,3335,3,478,239,
      0,3334,3329,1,0,0,0,3334,3330,1,0,0,0,3334,3331,1,0,0,0,3334,3332,
      1,0,0,0,3334,3333,1,0,0,0,3335,445,1,0,0,0,3336,3346,3,480,240,0,3337,
      3346,3,482,241,0,3338,3346,3,484,242,0,3339,3346,3,486,243,0,3340,
      3346,3,488,244,0,3341,3346,3,490,245,0,3342,3346,3,492,246,0,3343,
      3346,3,496,248,0,3344,3346,3,498,249,0,3345,3336,1,0,0,0,3345,3337,
      1,0,0,0,3345,3338,1,0,0,0,3345,3339,1,0,0,0,3345,3340,1,0,0,0,3345,
      3341,1,0,0,0,3345,3342,1,0,0,0,3345,3343,1,0,0,0,3345,3344,1,0,0,0,
      3346,447,1,0,0,0,3347,3350,3,500,250,0,3348,3350,3,502,251,0,3349,
      3347,1,0,0,0,3349,3348,1,0,0,0,3350,449,1,0,0,0,3351,3355,3,504,252,
      0,3352,3355,3,506,253,0,3353,3355,3,508,254,0,3354,3351,1,0,0,0,3354,
      3352,1,0,0,0,3354,3353,1,0,0,0,3355,451,1,0,0,0,3356,3357,3,510,255,
      0,3357,3358,5,317,0,0,3358,3359,5,47,0,0,3359,453,1,0,0,0,3360,3361,
      3,510,255,0,3361,3362,5,317,0,0,3362,3363,5,81,0,0,3363,455,1,0,0,
      0,3364,3365,3,510,255,0,3365,3366,5,317,0,0,3366,3367,5,214,0,0,3367,
      457,1,0,0,0,3368,3369,3,512,256,0,3369,3370,5,317,0,0,3370,3371,5,
      81,0,0,3371,459,1,0,0,0,3372,3373,3,512,256,0,3373,3374,5,317,0,0,
      3374,3375,5,256,0,0,3375,461,1,0,0,0,3376,3377,3,512,256,0,3377,3378,
      5,317,0,0,3378,3379,5,214,0,0,3379,463,1,0,0,0,3380,3381,3,514,257,
      0,3381,3382,5,317,0,0,3382,3383,5,81,0,0,3383,465,1,0,0,0,3384,3385,
      3,514,257,0,3385,3386,5,317,0,0,3386,3387,5,256,0,0,3387,467,1,0,0,
      0,3388,3389,3,514,257,0,3389,3390,5,317,0,0,3390,3391,5,214,0,0,3391,
      469,1,0,0,0,3392,3393,3,516,258,0,3393,3394,5,317,0,0,3394,3395,5,
      253,0,0,3395,471,1,0,0,0,3396,3397,3,516,258,0,3397,3398,5,317,0,0,
      3398,3399,5,81,0,0,3399,473,1,0,0,0,3400,3401,3,516,258,0,3401,3402,
      5,317,0,0,3402,3403,5,69,0,0,3403,475,1,0,0,0,3404,3405,3,516,258,
      0,3405,3406,5,317,0,0,3406,3407,5,159,0,0,3407,477,1,0,0,0,3408,3409,
      3,516,258,0,3409,3410,5,317,0,0,3410,3411,5,123,0,0,3411,479,1,0,0,
      0,3412,3413,3,518,259,0,3413,3414,5,317,0,0,3414,3415,5,259,0,0,3415,
      481,1,0,0,0,3416,3417,3,518,259,0,3417,3418,5,317,0,0,3418,3419,5,
      174,0,0,3419,483,1,0,0,0,3420,3421,3,518,259,0,3421,3422,5,317,0,0,
      3422,3423,5,119,0,0,3423,485,1,0,0,0,3424,3425,3,518,259,0,3425,3426,
      5,317,0,0,3426,3427,5,140,0,0,3427,487,1,0,0,0,3428,3429,3,518,259,
      0,3429,3430,5,317,0,0,3430,3431,5,139,0,0,3431,489,1,0,0,0,3432,3433,
      3,518,259,0,3433,3434,5,317,0,0,3434,3435,5,282,0,0,3435,491,1,0,0,
      0,3436,3437,3,518,259,0,3437,3438,5,317,0,0,3438,3439,5,5,0,0,3439,
      493,1,0,0,0,3440,3441,3,518,259,0,3441,3442,5,317,0,0,3442,3443,5,
      125,0,0,3443,495,1,0,0,0,3444,3445,3,518,259,0,3445,3446,5,317,0,0,
      3446,3447,5,145,0,0,3447,497,1,0,0,0,3448,3449,3,518,259,0,3449,3450,
      5,317,0,0,3450,3451,5,154,0,0,3451,499,1,0,0,0,3452,3453,3,520,260,
      0,3453,3454,5,317,0,0,3454,3455,5,220,0,0,3455,501,1,0,0,0,3456,3457,
      3,520,260,0,3457,3458,5,317,0,0,3458,3459,5,300,0,0,3459,503,1,0,0,
      0,3460,3461,3,520,260,0,3461,3462,5,317,0,0,3462,3463,5,106,0,0,3463,
      505,1,0,0,0,3464,3465,3,520,260,0,3465,3466,5,317,0,0,3466,3467,5,
      103,0,0,3467,507,1,0,0,0,3468,3469,3,520,260,0,3469,3470,5,317,0,0,
      3470,3471,5,102,0,0,3471,509,1,0,0,0,3472,3473,5,51,0,0,3473,511,1,
      0,0,0,3474,3475,5,202,0,0,3475,513,1,0,0,0,3476,3477,5,206,0,0,3477,
      515,1,0,0,0,3478,3479,5,234,0,0,3479,517,1,0,0,0,3480,3481,5,292,0,
      0,3481,519,1,0,0,0,3482,3483,5,134,0,0,3483,521,1,0,0,0,3484,3487,
      3,588,294,0,3485,3487,3,610,305,0,3486,3484,1,0,0,0,3486,3485,1,0,
      0,0,3487,523,1,0,0,0,3488,3491,3,582,291,0,3489,3491,3,610,305,0,3490,
      3488,1,0,0,0,3490,3489,1,0,0,0,3491,525,1,0,0,0,3492,3495,3,578,289,
      0,3493,3495,3,610,305,0,3494,3492,1,0,0,0,3494,3493,1,0,0,0,3495,527,
      1,0,0,0,3496,3499,3,580,290,0,3497,3499,3,610,305,0,3498,3496,1,0,
      0,0,3498,3497,1,0,0,0,3499,529,1,0,0,0,3500,3505,3,528,264,0,3501,
      3502,5,316,0,0,3502,3504,3,528,264,0,3503,3501,1,0,0,0,3504,3507,1,
      0,0,0,3505,3503,1,0,0,0,3505,3506,1,0,0,0,3506,3509,1,0,0,0,3507,3505,
      1,0,0,0,3508,3500,1,0,0,0,3508,3509,1,0,0,0,3509,531,1,0,0,0,3510,
      3513,3,590,295,0,3511,3513,3,610,305,0,3512,3510,1,0,0,0,3512,3511,
      1,0,0,0,3513,533,1,0,0,0,3514,3517,3,570,285,0,3515,3517,3,610,305,
      0,3516,3514,1,0,0,0,3516,3515,1,0,0,0,3517,535,1,0,0,0,3518,3523,3,
      534,267,0,3519,3520,5,316,0,0,3520,3522,3,534,267,0,3521,3519,1,0,
      0,0,3522,3525,1,0,0,0,3523,3521,1,0,0,0,3523,3524,1,0,0,0,3524,3527,
      1,0,0,0,3525,3523,1,0,0,0,3526,3518,1,0,0,0,3526,3527,1,0,0,0,3527,
      537,1,0,0,0,3528,3531,3,572,286,0,3529,3531,3,610,305,0,3530,3528,
      1,0,0,0,3530,3529,1,0,0,0,3531,539,1,0,0,0,3532,3535,3,562,281,0,3533,
      3535,3,610,305,0,3534,3532,1,0,0,0,3534,3533,1,0,0,0,3535,541,1,0,
      0,0,3536,3539,3,572,286,0,3537,3539,3,592,296,0,3538,3536,1,0,0,0,
      3538,3537,1,0,0,0,3539,543,1,0,0,0,3540,3542,3,546,273,0,3541,3540,
      1,0,0,0,3541,3542,1,0,0,0,3542,545,1,0,0,0,3543,3548,3,328,164,0,3544,
      3545,5,316,0,0,3545,3547,3,328,164,0,3546,3544,1,0,0,0,3547,3550,1,
      0,0,0,3548,3546,1,0,0,0,3548,3549,1,0,0,0,3549,547,1,0,0,0,3550,3548,
      1,0,0,0,3551,3553,3,550,275,0,3552,3551,1,0,0,0,3552,3553,1,0,0,0,
      3553,549,1,0,0,0,3554,3559,3,608,304,0,3555,3556,5,316,0,0,3556,3558,
      3,608,304,0,3557,3555,1,0,0,0,3558,3561,1,0,0,0,3559,3557,1,0,0,0,
      3559,3560,1,0,0,0,3560,551,1,0,0,0,3561,3559,1,0,0,0,3562,3564,3,554,
      277,0,3563,3562,1,0,0,0,3563,3564,1,0,0,0,3564,553,1,0,0,0,3565,3570,
      3,52,26,0,3566,3567,5,316,0,0,3567,3569,3,52,26,0,3568,3566,1,0,0,
      0,3569,3572,1,0,0,0,3570,3568,1,0,0,0,3570,3571,1,0,0,0,3571,555,1,
      0,0,0,3572,3570,1,0,0,0,3573,3582,5,313,0,0,3574,3579,3,570,285,0,
      3575,3576,5,316,0,0,3576,3578,3,570,285,0,3577,3575,1,0,0,0,3578,3581,
      1,0,0,0,3579,3577,1,0,0,0,3579,3580,1,0,0,0,3580,3583,1,0,0,0,3581,
      3579,1,0,0,0,3582,3574,1,0,0,0,3582,3583,1,0,0,0,3583,3584,1,0,0,0,
      3584,3585,5,314,0,0,3585,557,1,0,0,0,3586,3588,3,560,280,0,3587,3586,
      1,0,0,0,3587,3588,1,0,0,0,3588,559,1,0,0,0,3589,3594,3,570,285,0,3590,
      3591,5,316,0,0,3591,3593,3,570,285,0,3592,3590,1,0,0,0,3593,3596,1,
      0,0,0,3594,3592,1,0,0,0,3594,3595,1,0,0,0,3595,561,1,0,0,0,3596,3594,
      1,0,0,0,3597,3600,3,572,286,0,3598,3600,3,592,296,0,3599,3597,1,0,
      0,0,3599,3598,1,0,0,0,3600,563,1,0,0,0,3601,3602,3,582,291,0,3602,
      3603,5,317,0,0,3603,3604,5,317,0,0,3604,3605,3,582,291,0,3605,3612,
      1,0,0,0,3606,3607,3,578,289,0,3607,3608,5,317,0,0,3608,3609,5,317,
      0,0,3609,3610,3,578,289,0,3610,3612,1,0,0,0,3611,3601,1,0,0,0,3611,
      3606,1,0,0,0,3612,565,1,0,0,0,3613,3622,5,311,0,0,3614,3619,3,570,
      285,0,3615,3616,5,316,0,0,3616,3618,3,570,285,0,3617,3615,1,0,0,0,
      3618,3621,1,0,0,0,3619,3617,1,0,0,0,3619,3620,1,0,0,0,3620,3623,1,
      0,0,0,3621,3619,1,0,0,0,3622,3614,1,0,0,0,3622,3623,1,0,0,0,3623,3624,
      1,0,0,0,3624,3625,5,312,0,0,3625,567,1,0,0,0,3626,3631,3,580,290,0,
      3627,3628,5,316,0,0,3628,3630,3,580,290,0,3629,3627,1,0,0,0,3630,3633,
      1,0,0,0,3631,3629,1,0,0,0,3631,3632,1,0,0,0,3632,3635,1,0,0,0,3633,
      3631,1,0,0,0,3634,3626,1,0,0,0,3634,3635,1,0,0,0,3635,569,1,0,0,0,
      3636,3659,3,586,293,0,3637,3659,3,588,294,0,3638,3659,3,578,289,0,
      3639,3659,3,590,295,0,3640,3659,3,592,296,0,3641,3659,3,336,168,0,
      3642,3659,3,352,176,0,3643,3659,3,346,173,0,3644,3659,3,342,171,0,
      3645,3659,3,360,180,0,3646,3659,3,362,181,0,3647,3659,3,364,182,0,
      3648,3659,3,566,283,0,3649,3659,3,556,278,0,3650,3659,3,564,282,0,
      3651,3659,3,52,26,0,3652,3659,3,54,27,0,3653,3659,3,598,299,0,3654,
      3659,3,600,300,0,3655,3659,3,602,301,0,3656,3659,3,604,302,0,3657,
      3659,3,572,286,0,3658,3636,1,0,0,0,3658,3637,1,0,0,0,3658,3638,1,0,
      0,0,3658,3639,1,0,0,0,3658,3640,1,0,0,0,3658,3641,1,0,0,0,3658,3642,
      1,0,0,0,3658,3643,1,0,0,0,3658,3644,1,0,0,0,3658,3645,1,0,0,0,3658,
      3646,1,0,0,0,3658,3647,1,0,0,0,3658,3648,1,0,0,0,3658,3649,1,0,0,0,
      3658,3650,1,0,0,0,3658,3651,1,0,0,0,3658,3652,1,0,0,0,3658,3653,1,
      0,0,0,3658,3654,1,0,0,0,3658,3655,1,0,0,0,3658,3656,1,0,0,0,3658,3657,
      1,0,0,0,3659,571,1,0,0,0,3660,3661,5,313,0,0,3661,3662,5,318,0,0,3662,
      3675,5,314,0,0,3663,3664,5,313,0,0,3664,3669,3,576,288,0,3665,3666,
      5,316,0,0,3666,3668,3,576,288,0,3667,3665,1,0,0,0,3668,3671,1,0,0,
      0,3669,3667,1,0,0,0,3669,3670,1,0,0,0,3670,3672,1,0,0,0,3671,3669,
      1,0,0,0,3672,3673,5,314,0,0,3673,3675,1,0,0,0,3674,3660,1,0,0,0,3674,
      3663,1,0,0,0,3675,573,1,0,0,0,3676,3677,5,309,0,0,3677,3678,3,336,
      168,0,3678,3679,5,310,0,0,3679,3682,1,0,0,0,3680,3682,3,340,170,0,
      3681,3676,1,0,0,0,3681,3680,1,0,0,0,3682,3730,1,0,0,0,3683,3684,5,
      309,0,0,3684,3685,3,346,173,0,3685,3686,5,310,0,0,3686,3689,1,0,0,
      0,3687,3689,3,350,175,0,3688,3683,1,0,0,0,3688,3687,1,0,0,0,3689,3730,
      1,0,0,0,3690,3691,5,309,0,0,3691,3692,3,566,283,0,3692,3693,5,310,
      0,0,3693,3696,1,0,0,0,3694,3696,3,566,283,0,3695,3690,1,0,0,0,3695,
      3694,1,0,0,0,3696,3730,1,0,0,0,3697,3698,5,309,0,0,3698,3699,3,556,
      278,0,3699,3700,5,310,0,0,3700,3703,1,0,0,0,3701,3703,3,556,278,0,
      3702,3697,1,0,0,0,3702,3701,1,0,0,0,3703,3730,1,0,0,0,3704,3705,5,
      309,0,0,3705,3706,3,572,286,0,3706,3707,5,310,0,0,3707,3710,1,0,0,
      0,3708,3710,3,572,286,0,3709,3704,1,0,0,0,3709,3708,1,0,0,0,3710,3730,
      1,0,0,0,3711,3712,5,309,0,0,3712,3713,3,578,289,0,3713,3714,5,310,
      0,0,3714,3717,1,0,0,0,3715,3717,3,578,289,0,3716,3711,1,0,0,0,3716,
      3715,1,0,0,0,3717,3730,1,0,0,0,3718,3719,5,309,0,0,3719,3720,3,586,
      293,0,3720,3721,5,310,0,0,3721,3724,1,0,0,0,3722,3724,3,586,293,0,
      3723,3718,1,0,0,0,3723,3722,1,0,0,0,3724,3730,1,0,0,0,3725,3728,3,
      612,306,0,3726,3728,3,606,303,0,3727,3725,1,0,0,0,3727,3726,1,0,0,
      0,3728,3730,1,0,0,0,3729,3681,1,0,0,0,3729,3688,1,0,0,0,3729,3695,
      1,0,0,0,3729,3702,1,0,0,0,3729,3709,1,0,0,0,3729,3716,1,0,0,0,3729,
      3723,1,0,0,0,3729,3727,1,0,0,0,3730,575,1,0,0,0,3731,3732,3,574,287,
      0,3732,3733,5,318,0,0,3733,3734,3,570,285,0,3734,577,1,0,0,0,3735,
      3736,7,2,0,0,3736,579,1,0,0,0,3737,3740,3,578,289,0,3738,3740,5,183,
      0,0,3739,3737,1,0,0,0,3739,3738,1,0,0,0,3740,581,1,0,0,0,3741,3742,
      5,301,0,0,3742,583,1,0,0,0,3743,3747,5,302,0,0,3744,3747,3,596,298,
      0,3745,3747,3,594,297,0,3746,3743,1,0,0,0,3746,3744,1,0,0,0,3746,3745,
      1,0,0,0,3747,585,1,0,0,0,3748,3751,3,582,291,0,3749,3751,3,584,292,
      0,3750,3748,1,0,0,0,3750,3749,1,0,0,0,3751,587,1,0,0,0,3752,3753,7,
      3,0,0,3753,589,1,0,0,0,3754,3755,5,59,0,0,3755,3756,5,309,0,0,3756,
      3757,3,578,289,0,3757,3758,5,310,0,0,3758,3771,1,0,0,0,3759,3760,5,
      59,0,0,3760,3761,5,309,0,0,3761,3771,5,310,0,0,3762,3763,5,60,0,0,
      3763,3764,5,309,0,0,3764,3765,3,578,289,0,3765,3766,5,310,0,0,3766,
      3771,1,0,0,0,3767,3768,5,60,0,0,3768,3769,5,309,0,0,3769,3771,5,310,
      0,0,3770,3754,1,0,0,0,3770,3759,1,0,0,0,3770,3762,1,0,0,0,3770,3767,
      1,0,0,0,3771,591,1,0,0,0,3772,3773,5,183,0,0,3773,593,1,0,0,0,3774,
      3775,5,171,0,0,3775,595,1,0,0,0,3776,3777,7,4,0,0,3777,597,1,0,0,0,
      3778,3779,5,278,0,0,3779,3780,5,309,0,0,3780,3787,5,310,0,0,3781,3782,
      5,278,0,0,3782,3783,5,309,0,0,3783,3784,3,578,289,0,3784,3785,5,310,
      0,0,3785,3787,1,0,0,0,3786,3778,1,0,0,0,3786,3781,1,0,0,0,3787,599,
      1,0,0,0,3788,3789,5,304,0,0,3789,601,1,0,0,0,3790,3791,5,76,0,0,3791,
      3792,5,309,0,0,3792,3793,3,582,291,0,3793,3794,5,316,0,0,3794,3797,
      3,582,291,0,3795,3796,5,316,0,0,3796,3798,3,588,294,0,3797,3795,1,
      0,0,0,3797,3798,1,0,0,0,3798,3799,1,0,0,0,3799,3800,5,310,0,0,3800,
      603,1,0,0,0,3801,3802,5,24,0,0,3802,3803,5,309,0,0,3803,3804,3,578,
      289,0,3804,3805,5,310,0,0,3805,605,1,0,0,0,3806,3807,5,323,0,0,3807,
      607,1,0,0,0,3808,3809,5,323,0,0,3809,609,1,0,0,0,3810,3811,5,323,0,
      0,3811,611,1,0,0,0,3812,3813,7,5,0,0,3813,613,1,0,0,0,214,616,621,
      625,641,650,660,667,691,703,712,735,753,760,769,785,797,809,824,856,
      868,901,916,923,1064,1086,1101,1128,1148,1158,1173,1248,1278,1285,
      1331,1353,1362,1386,1409,1416,1425,1467,1477,1489,1506,1523,1533,1543,
      1596,1603,1612,1624,1628,1640,1644,1656,1665,1713,1731,1745,1760,1770,
      1795,1805,1825,1840,1850,1904,1924,1952,1975,1982,2000,2018,2027,2050,
      2068,2086,2100,2110,2124,2147,2170,2179,2206,2220,2255,2265,2287,2304,
      2318,2325,2337,2347,2362,2372,2393,2409,2413,2437,2443,2451,2457,2461,
      2468,2477,2480,2483,2487,2500,2506,2510,2526,2544,2558,2562,2581,2587,
      2597,2607,2625,2635,2653,2699,2713,2731,2949,2973,2994,2996,3007,3015,
      3023,3033,3043,3053,3059,3065,3075,3085,3095,3105,3117,3129,3141,3149,
      3155,3161,3169,3175,3181,3191,3201,3211,3221,3231,3241,3251,3261,3291,
      3312,3317,3322,3327,3334,3345,3349,3354,3486,3490,3494,3498,3505,3508,
      3512,3516,3523,3526,3530,3534,3538,3541,3548,3552,3559,3563,3570,3579,
      3582,3587,3594,3599,3611,3619,3622,3631,3634,3658,3669,3674,3681,3688,
      3695,3702,3709,3716,3723,3727,3729,3739,3746,3750,3770,3786,3797
  ];

  static final ATN _ATN =
      ATNDeserializer().deserialize(_serializedATN);
}
class QueryListContext extends ParserRuleContext {
  List<QueryContext> querys() => getRuleContexts<QueryContext>();
  QueryContext? query(int i) => getRuleContext<QueryContext>(i);
  TerminalNode? EOF() => getToken(GremlinParser.TOKEN_EOF, 0);
  List<TerminalNode> SEMIs() => getTokens(GremlinParser.TOKEN_SEMI);
  TerminalNode? SEMI(int i) => getToken(GremlinParser.TOKEN_SEMI, i);
  QueryListContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_queryList;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitQueryList(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class QueryContext extends ParserRuleContext {
  TraversalSourceContext? traversalSource() => getRuleContext<TraversalSourceContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TransactionPartContext? transactionPart() => getRuleContext<TransactionPartContext>(0);
  RootTraversalContext? rootTraversal() => getRuleContext<RootTraversalContext>(0);
  TraversalTerminalMethodContext? traversalTerminalMethod() => getRuleContext<TraversalTerminalMethodContext>(0);
  EmptyQueryContext? emptyQuery() => getRuleContext<EmptyQueryContext>(0);
  QueryContext? query() => getRuleContext<QueryContext>(0);
  TerminalNode? K_TOSTRING() => getToken(GremlinParser.TOKEN_K_TOSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  QueryContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_query;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitQuery(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class EmptyQueryContext extends ParserRuleContext {
  TerminalNode? EmptyStringLiteral() => getToken(GremlinParser.TOKEN_EmptyStringLiteral, 0);
  EmptyQueryContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_emptyQuery;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitEmptyQuery(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceContext extends ParserRuleContext {
  TerminalNode? TRAVERSAL_ROOT() => getToken(GremlinParser.TOKEN_TRAVERSAL_ROOT, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TraversalSourceSelfMethodContext? traversalSourceSelfMethod() => getRuleContext<TraversalSourceSelfMethodContext>(0);
  TraversalSourceContext? traversalSource() => getRuleContext<TraversalSourceContext>(0);
  TraversalSourceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSource;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSource(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TransactionPartContext extends ParserRuleContext {
  TerminalNode? K_TX() => getToken(GremlinParser.TOKEN_K_TX, 0);
  List<TerminalNode> LPARENs() => getTokens(GremlinParser.TOKEN_LPAREN);
  TerminalNode? LPAREN(int i) => getToken(GremlinParser.TOKEN_LPAREN, i);
  List<TerminalNode> RPARENs() => getTokens(GremlinParser.TOKEN_RPAREN);
  TerminalNode? RPAREN(int i) => getToken(GremlinParser.TOKEN_RPAREN, i);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_BEGIN() => getToken(GremlinParser.TOKEN_K_BEGIN, 0);
  TerminalNode? K_COMMIT() => getToken(GremlinParser.TOKEN_K_COMMIT, 0);
  TerminalNode? K_ROLLBACK() => getToken(GremlinParser.TOKEN_K_ROLLBACK, 0);
  TransactionPartContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_transactionPart;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTransactionPart(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class RootTraversalContext extends ParserRuleContext {
  TraversalSourceContext? traversalSource() => getRuleContext<TraversalSourceContext>(0);
  List<TerminalNode> DOTs() => getTokens(GremlinParser.TOKEN_DOT);
  TerminalNode? DOT(int i) => getToken(GremlinParser.TOKEN_DOT, i);
  TraversalSourceSpawnMethodContext? traversalSourceSpawnMethod() => getRuleContext<TraversalSourceSpawnMethodContext>(0);
  ChainedTraversalContext? chainedTraversal() => getRuleContext<ChainedTraversalContext>(0);
  RootTraversalContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_rootTraversal;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitRootTraversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethodContext extends ParserRuleContext {
  TraversalSourceSelfMethod_withBulkContext? traversalSourceSelfMethod_withBulk() => getRuleContext<TraversalSourceSelfMethod_withBulkContext>(0);
  TraversalSourceSelfMethod_withPathContext? traversalSourceSelfMethod_withPath() => getRuleContext<TraversalSourceSelfMethod_withPathContext>(0);
  TraversalSourceSelfMethod_withSackContext? traversalSourceSelfMethod_withSack() => getRuleContext<TraversalSourceSelfMethod_withSackContext>(0);
  TraversalSourceSelfMethod_withSideEffectContext? traversalSourceSelfMethod_withSideEffect() => getRuleContext<TraversalSourceSelfMethod_withSideEffectContext>(0);
  TraversalSourceSelfMethod_withStrategiesContext? traversalSourceSelfMethod_withStrategies() => getRuleContext<TraversalSourceSelfMethod_withStrategiesContext>(0);
  TraversalSourceSelfMethod_withoutStrategiesContext? traversalSourceSelfMethod_withoutStrategies() => getRuleContext<TraversalSourceSelfMethod_withoutStrategiesContext>(0);
  TraversalSourceSelfMethod_withContext? traversalSourceSelfMethod_with() => getRuleContext<TraversalSourceSelfMethod_withContext>(0);
  TraversalSourceSelfMethodContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withBulkContext extends ParserRuleContext {
  TerminalNode? K_WITHBULK() => getToken(GremlinParser.TOKEN_K_WITHBULK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  BooleanLiteralContext? booleanLiteral() => getRuleContext<BooleanLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSelfMethod_withBulkContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withBulk;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withBulk(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withPathContext extends ParserRuleContext {
  TerminalNode? K_WITHPATH() => getToken(GremlinParser.TOKEN_K_WITHPATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSelfMethod_withPathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withPath;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withPath(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withSackContext extends ParserRuleContext {
  TerminalNode? K_WITHSACK() => getToken(GremlinParser.TOKEN_K_WITHSACK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalBiFunctionContext? traversalBiFunction() => getRuleContext<TraversalBiFunctionContext>(0);
  TraversalSourceSelfMethod_withSackContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withSack;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withSack(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withSideEffectContext extends ParserRuleContext {
  TerminalNode? K_WITHSIDEEFFECT() => getToken(GremlinParser.TOKEN_K_WITHSIDEEFFECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalBiFunctionContext? traversalBiFunction() => getRuleContext<TraversalBiFunctionContext>(0);
  TraversalSourceSelfMethod_withSideEffectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withSideEffect;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withSideEffect(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withStrategiesContext extends ParserRuleContext {
  TerminalNode? K_WITHSTRATEGIES() => getToken(GremlinParser.TOKEN_K_WITHSTRATEGIES, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalStrategyContext? traversalStrategy() => getRuleContext<TraversalStrategyContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalStrategyVarargsContext? traversalStrategyVarargs() => getRuleContext<TraversalStrategyVarargsContext>(0);
  TraversalSourceSelfMethod_withStrategiesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withStrategies;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withStrategies(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withoutStrategiesContext extends ParserRuleContext {
  TerminalNode? K_WITHOUTSTRATEGIES() => getToken(GremlinParser.TOKEN_K_WITHOUTSTRATEGIES, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  ClassTypeContext? classType() => getRuleContext<ClassTypeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  ClassTypeListContext? classTypeList() => getRuleContext<ClassTypeListContext>(0);
  TraversalSourceSelfMethod_withoutStrategiesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_withoutStrategies;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_withoutStrategies(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSelfMethod_withContext extends ParserRuleContext {
  TerminalNode? K_WITH() => getToken(GremlinParser.TOKEN_K_WITH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TraversalSourceSelfMethod_withContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSelfMethod_with;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSelfMethod_with(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethodContext extends ParserRuleContext {
  TraversalSourceSpawnMethod_addEContext? traversalSourceSpawnMethod_addE() => getRuleContext<TraversalSourceSpawnMethod_addEContext>(0);
  TraversalSourceSpawnMethod_addVContext? traversalSourceSpawnMethod_addV() => getRuleContext<TraversalSourceSpawnMethod_addVContext>(0);
  TraversalSourceSpawnMethod_EContext? traversalSourceSpawnMethod_E() => getRuleContext<TraversalSourceSpawnMethod_EContext>(0);
  TraversalSourceSpawnMethod_VContext? traversalSourceSpawnMethod_V() => getRuleContext<TraversalSourceSpawnMethod_VContext>(0);
  TraversalSourceSpawnMethod_mergeEContext? traversalSourceSpawnMethod_mergeE() => getRuleContext<TraversalSourceSpawnMethod_mergeEContext>(0);
  TraversalSourceSpawnMethod_mergeVContext? traversalSourceSpawnMethod_mergeV() => getRuleContext<TraversalSourceSpawnMethod_mergeVContext>(0);
  TraversalSourceSpawnMethod_injectContext? traversalSourceSpawnMethod_inject() => getRuleContext<TraversalSourceSpawnMethod_injectContext>(0);
  TraversalSourceSpawnMethod_ioContext? traversalSourceSpawnMethod_io() => getRuleContext<TraversalSourceSpawnMethod_ioContext>(0);
  TraversalSourceSpawnMethod_callContext? traversalSourceSpawnMethod_call() => getRuleContext<TraversalSourceSpawnMethod_callContext>(0);
  TraversalSourceSpawnMethod_unionContext? traversalSourceSpawnMethod_union() => getRuleContext<TraversalSourceSpawnMethod_unionContext>(0);
  TraversalSourceSpawnMethodContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_addEContext extends ParserRuleContext {
  TerminalNode? K_ADDE() => getToken(GremlinParser.TOKEN_K_ADDE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TraversalSourceSpawnMethod_addEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_addE;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_addE(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_addVContext extends ParserRuleContext {
  TerminalNode? K_ADDV() => getToken(GremlinParser.TOKEN_K_ADDV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TraversalSourceSpawnMethod_addVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_addV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_addV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_EContext extends ParserRuleContext {
  TerminalNode? K_E() => getToken(GremlinParser.TOKEN_K_E, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_EContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_E;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_E(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_VContext extends ParserRuleContext {
  TerminalNode? K_V() => getToken(GremlinParser.TOKEN_K_V, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_VContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_V;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_V(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_injectContext extends ParserRuleContext {
  TerminalNode? K_INJECT() => getToken(GremlinParser.TOKEN_K_INJECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralVarargsContext? genericLiteralVarargs() => getRuleContext<GenericLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_injectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_inject;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_inject(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_ioContext extends ParserRuleContext {
  TerminalNode? K_IO() => getToken(GremlinParser.TOKEN_K_IO, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_ioContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_io;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_io(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_mergeVContext extends ParserRuleContext {
  TraversalSourceSpawnMethod_mergeVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_mergeV;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalSourceSpawnMethod_mergeEContext extends ParserRuleContext {
  TraversalSourceSpawnMethod_mergeEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_mergeE;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalSourceSpawnMethod_callContext extends ParserRuleContext {
  TraversalSourceSpawnMethod_callContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_call;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalSourceSpawnMethod_unionContext extends ParserRuleContext {
  TerminalNode? K_UNION() => getToken(GremlinParser.TOKEN_K_UNION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_unionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSourceSpawnMethod_union;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_union(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ChainedTraversalContext extends ParserRuleContext {
  TraversalMethodContext? traversalMethod() => getRuleContext<TraversalMethodContext>(0);
  ChainedTraversalContext? chainedTraversal() => getRuleContext<ChainedTraversalContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  ChainedTraversalContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_chainedTraversal;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitChainedTraversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NestedTraversalContext extends ParserRuleContext {
  ChainedTraversalContext? chainedTraversal() => getRuleContext<ChainedTraversalContext>(0);
  TerminalNode? ANON_TRAVERSAL_ROOT() => getToken(GremlinParser.TOKEN_ANON_TRAVERSAL_ROOT, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  NestedTraversalContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nestedTraversal;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNestedTraversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TerminatedTraversalContext extends ParserRuleContext {
  RootTraversalContext? rootTraversal() => getRuleContext<RootTraversalContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TraversalTerminalMethodContext? traversalTerminalMethod() => getRuleContext<TraversalTerminalMethodContext>(0);
  TerminatedTraversalContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_terminatedTraversal;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTerminatedTraversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethodContext extends ParserRuleContext {
  TraversalMethod_VContext? traversalMethod_V() => getRuleContext<TraversalMethod_VContext>(0);
  TraversalMethod_EContext? traversalMethod_E() => getRuleContext<TraversalMethod_EContext>(0);
  TraversalMethod_addEContext? traversalMethod_addE() => getRuleContext<TraversalMethod_addEContext>(0);
  TraversalMethod_addVContext? traversalMethod_addV() => getRuleContext<TraversalMethod_addVContext>(0);
  TraversalMethod_mergeEContext? traversalMethod_mergeE() => getRuleContext<TraversalMethod_mergeEContext>(0);
  TraversalMethod_mergeVContext? traversalMethod_mergeV() => getRuleContext<TraversalMethod_mergeVContext>(0);
  TraversalMethod_aggregateContext? traversalMethod_aggregate() => getRuleContext<TraversalMethod_aggregateContext>(0);
  TraversalMethod_allContext? traversalMethod_all() => getRuleContext<TraversalMethod_allContext>(0);
  TraversalMethod_andContext? traversalMethod_and() => getRuleContext<TraversalMethod_andContext>(0);
  TraversalMethod_anyContext? traversalMethod_any() => getRuleContext<TraversalMethod_anyContext>(0);
  TraversalMethod_asContext? traversalMethod_as() => getRuleContext<TraversalMethod_asContext>(0);
  TraversalMethod_barrierContext? traversalMethod_barrier() => getRuleContext<TraversalMethod_barrierContext>(0);
  TraversalMethod_bothContext? traversalMethod_both() => getRuleContext<TraversalMethod_bothContext>(0);
  TraversalMethod_bothEContext? traversalMethod_bothE() => getRuleContext<TraversalMethod_bothEContext>(0);
  TraversalMethod_bothVContext? traversalMethod_bothV() => getRuleContext<TraversalMethod_bothVContext>(0);
  TraversalMethod_branchContext? traversalMethod_branch() => getRuleContext<TraversalMethod_branchContext>(0);
  TraversalMethod_byContext? traversalMethod_by() => getRuleContext<TraversalMethod_byContext>(0);
  TraversalMethod_capContext? traversalMethod_cap() => getRuleContext<TraversalMethod_capContext>(0);
  TraversalMethod_chooseContext? traversalMethod_choose() => getRuleContext<TraversalMethod_chooseContext>(0);
  TraversalMethod_coalesceContext? traversalMethod_coalesce() => getRuleContext<TraversalMethod_coalesceContext>(0);
  TraversalMethod_coinContext? traversalMethod_coin() => getRuleContext<TraversalMethod_coinContext>(0);
  TraversalMethod_conjoinContext? traversalMethod_conjoin() => getRuleContext<TraversalMethod_conjoinContext>(0);
  TraversalMethod_connectedComponentContext? traversalMethod_connectedComponent() => getRuleContext<TraversalMethod_connectedComponentContext>(0);
  TraversalMethod_constantContext? traversalMethod_constant() => getRuleContext<TraversalMethod_constantContext>(0);
  TraversalMethod_countContext? traversalMethod_count() => getRuleContext<TraversalMethod_countContext>(0);
  TraversalMethod_cyclicPathContext? traversalMethod_cyclicPath() => getRuleContext<TraversalMethod_cyclicPathContext>(0);
  TraversalMethod_dedupContext? traversalMethod_dedup() => getRuleContext<TraversalMethod_dedupContext>(0);
  TraversalMethod_differenceContext? traversalMethod_difference() => getRuleContext<TraversalMethod_differenceContext>(0);
  TraversalMethod_discardContext? traversalMethod_discard() => getRuleContext<TraversalMethod_discardContext>(0);
  TraversalMethod_disjunctContext? traversalMethod_disjunct() => getRuleContext<TraversalMethod_disjunctContext>(0);
  TraversalMethod_dropContext? traversalMethod_drop() => getRuleContext<TraversalMethod_dropContext>(0);
  TraversalMethod_elementMapContext? traversalMethod_elementMap() => getRuleContext<TraversalMethod_elementMapContext>(0);
  TraversalMethod_emitContext? traversalMethod_emit() => getRuleContext<TraversalMethod_emitContext>(0);
  TraversalMethod_filterContext? traversalMethod_filter() => getRuleContext<TraversalMethod_filterContext>(0);
  TraversalMethod_flatMapContext? traversalMethod_flatMap() => getRuleContext<TraversalMethod_flatMapContext>(0);
  TraversalMethod_foldContext? traversalMethod_fold() => getRuleContext<TraversalMethod_foldContext>(0);
  TraversalMethod_fromContext? traversalMethod_from() => getRuleContext<TraversalMethod_fromContext>(0);
  TraversalMethod_groupContext? traversalMethod_group() => getRuleContext<TraversalMethod_groupContext>(0);
  TraversalMethod_groupCountContext? traversalMethod_groupCount() => getRuleContext<TraversalMethod_groupCountContext>(0);
  TraversalMethod_hasContext? traversalMethod_has() => getRuleContext<TraversalMethod_hasContext>(0);
  TraversalMethod_hasIdContext? traversalMethod_hasId() => getRuleContext<TraversalMethod_hasIdContext>(0);
  TraversalMethod_hasKeyContext? traversalMethod_hasKey() => getRuleContext<TraversalMethod_hasKeyContext>(0);
  TraversalMethod_hasLabelContext? traversalMethod_hasLabel() => getRuleContext<TraversalMethod_hasLabelContext>(0);
  TraversalMethod_hasNotContext? traversalMethod_hasNot() => getRuleContext<TraversalMethod_hasNotContext>(0);
  TraversalMethod_hasValueContext? traversalMethod_hasValue() => getRuleContext<TraversalMethod_hasValueContext>(0);
  TraversalMethod_idContext? traversalMethod_id() => getRuleContext<TraversalMethod_idContext>(0);
  TraversalMethod_identityContext? traversalMethod_identity() => getRuleContext<TraversalMethod_identityContext>(0);
  TraversalMethod_inContext? traversalMethod_in() => getRuleContext<TraversalMethod_inContext>(0);
  TraversalMethod_inEContext? traversalMethod_inE() => getRuleContext<TraversalMethod_inEContext>(0);
  TraversalMethod_intersectContext? traversalMethod_intersect() => getRuleContext<TraversalMethod_intersectContext>(0);
  TraversalMethod_inVContext? traversalMethod_inV() => getRuleContext<TraversalMethod_inVContext>(0);
  TraversalMethod_indexContext? traversalMethod_index() => getRuleContext<TraversalMethod_indexContext>(0);
  TraversalMethod_injectContext? traversalMethod_inject() => getRuleContext<TraversalMethod_injectContext>(0);
  TraversalMethod_isContext? traversalMethod_is() => getRuleContext<TraversalMethod_isContext>(0);
  TraversalMethod_keyContext? traversalMethod_key() => getRuleContext<TraversalMethod_keyContext>(0);
  TraversalMethod_labelContext? traversalMethod_label() => getRuleContext<TraversalMethod_labelContext>(0);
  TraversalMethod_limitContext? traversalMethod_limit() => getRuleContext<TraversalMethod_limitContext>(0);
  TraversalMethod_localContext? traversalMethod_local() => getRuleContext<TraversalMethod_localContext>(0);
  TraversalMethod_loopsContext? traversalMethod_loops() => getRuleContext<TraversalMethod_loopsContext>(0);
  TraversalMethod_mapContext? traversalMethod_map() => getRuleContext<TraversalMethod_mapContext>(0);
  TraversalMethod_matchContext? traversalMethod_match() => getRuleContext<TraversalMethod_matchContext>(0);
  TraversalMethod_mathContext? traversalMethod_math() => getRuleContext<TraversalMethod_mathContext>(0);
  TraversalMethod_maxContext? traversalMethod_max() => getRuleContext<TraversalMethod_maxContext>(0);
  TraversalMethod_meanContext? traversalMethod_mean() => getRuleContext<TraversalMethod_meanContext>(0);
  TraversalMethod_minContext? traversalMethod_min() => getRuleContext<TraversalMethod_minContext>(0);
  TraversalMethod_noneContext? traversalMethod_none() => getRuleContext<TraversalMethod_noneContext>(0);
  TraversalMethod_notContext? traversalMethod_not() => getRuleContext<TraversalMethod_notContext>(0);
  TraversalMethod_optionContext? traversalMethod_option() => getRuleContext<TraversalMethod_optionContext>(0);
  TraversalMethod_optionalContext? traversalMethod_optional() => getRuleContext<TraversalMethod_optionalContext>(0);
  TraversalMethod_orContext? traversalMethod_or() => getRuleContext<TraversalMethod_orContext>(0);
  TraversalMethod_orderContext? traversalMethod_order() => getRuleContext<TraversalMethod_orderContext>(0);
  TraversalMethod_otherVContext? traversalMethod_otherV() => getRuleContext<TraversalMethod_otherVContext>(0);
  TraversalMethod_outContext? traversalMethod_out() => getRuleContext<TraversalMethod_outContext>(0);
  TraversalMethod_outEContext? traversalMethod_outE() => getRuleContext<TraversalMethod_outEContext>(0);
  TraversalMethod_outVContext? traversalMethod_outV() => getRuleContext<TraversalMethod_outVContext>(0);
  TraversalMethod_pageRankContext? traversalMethod_pageRank() => getRuleContext<TraversalMethod_pageRankContext>(0);
  TraversalMethod_pathContext? traversalMethod_path() => getRuleContext<TraversalMethod_pathContext>(0);
  TraversalMethod_peerPressureContext? traversalMethod_peerPressure() => getRuleContext<TraversalMethod_peerPressureContext>(0);
  TraversalMethod_profileContext? traversalMethod_profile() => getRuleContext<TraversalMethod_profileContext>(0);
  TraversalMethod_projectContext? traversalMethod_project() => getRuleContext<TraversalMethod_projectContext>(0);
  TraversalMethod_propertiesContext? traversalMethod_properties() => getRuleContext<TraversalMethod_propertiesContext>(0);
  TraversalMethod_propertyContext? traversalMethod_property() => getRuleContext<TraversalMethod_propertyContext>(0);
  TraversalMethod_propertyMapContext? traversalMethod_propertyMap() => getRuleContext<TraversalMethod_propertyMapContext>(0);
  TraversalMethod_rangeContext? traversalMethod_range() => getRuleContext<TraversalMethod_rangeContext>(0);
  TraversalMethod_readContext? traversalMethod_read() => getRuleContext<TraversalMethod_readContext>(0);
  TraversalMethod_repeatContext? traversalMethod_repeat() => getRuleContext<TraversalMethod_repeatContext>(0);
  TraversalMethod_sackContext? traversalMethod_sack() => getRuleContext<TraversalMethod_sackContext>(0);
  TraversalMethod_sampleContext? traversalMethod_sample() => getRuleContext<TraversalMethod_sampleContext>(0);
  TraversalMethod_selectContext? traversalMethod_select() => getRuleContext<TraversalMethod_selectContext>(0);
  TraversalMethod_combineContext? traversalMethod_combine() => getRuleContext<TraversalMethod_combineContext>(0);
  TraversalMethod_productContext? traversalMethod_product() => getRuleContext<TraversalMethod_productContext>(0);
  TraversalMethod_mergeContext? traversalMethod_merge() => getRuleContext<TraversalMethod_mergeContext>(0);
  TraversalMethod_shortestPathContext? traversalMethod_shortestPath() => getRuleContext<TraversalMethod_shortestPathContext>(0);
  TraversalMethod_sideEffectContext? traversalMethod_sideEffect() => getRuleContext<TraversalMethod_sideEffectContext>(0);
  TraversalMethod_simplePathContext? traversalMethod_simplePath() => getRuleContext<TraversalMethod_simplePathContext>(0);
  TraversalMethod_skipContext? traversalMethod_skip() => getRuleContext<TraversalMethod_skipContext>(0);
  TraversalMethod_subgraphContext? traversalMethod_subgraph() => getRuleContext<TraversalMethod_subgraphContext>(0);
  TraversalMethod_sumContext? traversalMethod_sum() => getRuleContext<TraversalMethod_sumContext>(0);
  TraversalMethod_tailContext? traversalMethod_tail() => getRuleContext<TraversalMethod_tailContext>(0);
  TraversalMethod_failContext? traversalMethod_fail() => getRuleContext<TraversalMethod_failContext>(0);
  TraversalMethod_timeLimitContext? traversalMethod_timeLimit() => getRuleContext<TraversalMethod_timeLimitContext>(0);
  TraversalMethod_timesContext? traversalMethod_times() => getRuleContext<TraversalMethod_timesContext>(0);
  TraversalMethod_toContext? traversalMethod_to() => getRuleContext<TraversalMethod_toContext>(0);
  TraversalMethod_toEContext? traversalMethod_toE() => getRuleContext<TraversalMethod_toEContext>(0);
  TraversalMethod_toVContext? traversalMethod_toV() => getRuleContext<TraversalMethod_toVContext>(0);
  TraversalMethod_treeContext? traversalMethod_tree() => getRuleContext<TraversalMethod_treeContext>(0);
  TraversalMethod_unfoldContext? traversalMethod_unfold() => getRuleContext<TraversalMethod_unfoldContext>(0);
  TraversalMethod_unionContext? traversalMethod_union() => getRuleContext<TraversalMethod_unionContext>(0);
  TraversalMethod_untilContext? traversalMethod_until() => getRuleContext<TraversalMethod_untilContext>(0);
  TraversalMethod_valueContext? traversalMethod_value() => getRuleContext<TraversalMethod_valueContext>(0);
  TraversalMethod_valueMapContext? traversalMethod_valueMap() => getRuleContext<TraversalMethod_valueMapContext>(0);
  TraversalMethod_valuesContext? traversalMethod_values() => getRuleContext<TraversalMethod_valuesContext>(0);
  TraversalMethod_whereContext? traversalMethod_where() => getRuleContext<TraversalMethod_whereContext>(0);
  TraversalMethod_withContext? traversalMethod_with() => getRuleContext<TraversalMethod_withContext>(0);
  TraversalMethod_writeContext? traversalMethod_write() => getRuleContext<TraversalMethod_writeContext>(0);
  TraversalMethod_elementContext? traversalMethod_element() => getRuleContext<TraversalMethod_elementContext>(0);
  TraversalMethod_callContext? traversalMethod_call() => getRuleContext<TraversalMethod_callContext>(0);
  TraversalMethod_concatContext? traversalMethod_concat() => getRuleContext<TraversalMethod_concatContext>(0);
  TraversalMethod_asStringContext? traversalMethod_asString() => getRuleContext<TraversalMethod_asStringContext>(0);
  TraversalMethod_formatContext? traversalMethod_format() => getRuleContext<TraversalMethod_formatContext>(0);
  TraversalMethod_toUpperContext? traversalMethod_toUpper() => getRuleContext<TraversalMethod_toUpperContext>(0);
  TraversalMethod_toLowerContext? traversalMethod_toLower() => getRuleContext<TraversalMethod_toLowerContext>(0);
  TraversalMethod_lengthContext? traversalMethod_length() => getRuleContext<TraversalMethod_lengthContext>(0);
  TraversalMethod_trimContext? traversalMethod_trim() => getRuleContext<TraversalMethod_trimContext>(0);
  TraversalMethod_lTrimContext? traversalMethod_lTrim() => getRuleContext<TraversalMethod_lTrimContext>(0);
  TraversalMethod_rTrimContext? traversalMethod_rTrim() => getRuleContext<TraversalMethod_rTrimContext>(0);
  TraversalMethod_reverseContext? traversalMethod_reverse() => getRuleContext<TraversalMethod_reverseContext>(0);
  TraversalMethod_replaceContext? traversalMethod_replace() => getRuleContext<TraversalMethod_replaceContext>(0);
  TraversalMethod_splitContext? traversalMethod_split() => getRuleContext<TraversalMethod_splitContext>(0);
  TraversalMethod_substringContext? traversalMethod_substring() => getRuleContext<TraversalMethod_substringContext>(0);
  TraversalMethod_asBoolContext? traversalMethod_asBool() => getRuleContext<TraversalMethod_asBoolContext>(0);
  TraversalMethod_asDateContext? traversalMethod_asDate() => getRuleContext<TraversalMethod_asDateContext>(0);
  TraversalMethod_dateAddContext? traversalMethod_dateAdd() => getRuleContext<TraversalMethod_dateAddContext>(0);
  TraversalMethod_dateDiffContext? traversalMethod_dateDiff() => getRuleContext<TraversalMethod_dateDiffContext>(0);
  TraversalMethod_asNumberContext? traversalMethod_asNumber() => getRuleContext<TraversalMethod_asNumberContext>(0);
  TraversalMethodContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_VContext extends ParserRuleContext {
  TerminalNode? K_V() => getToken(GremlinParser.TOKEN_K_V, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_VContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_V;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_V(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_EContext extends ParserRuleContext {
  TerminalNode? K_E() => getToken(GremlinParser.TOKEN_K_E, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_EContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_E;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_E(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_addEContext extends ParserRuleContext {
  TraversalMethod_addEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_addE;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_addVContext extends ParserRuleContext {
  TraversalMethod_addVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_addV;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_aggregateContext extends ParserRuleContext {
  TraversalMethod_aggregateContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_aggregate;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_allContext extends ParserRuleContext {
  TraversalMethod_allContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_all;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_andContext extends ParserRuleContext {
  TerminalNode? K_AND() => getToken(GremlinParser.TOKEN_K_AND, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_andContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_and;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_and(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_anyContext extends ParserRuleContext {
  TraversalMethod_anyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_any;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_asContext extends ParserRuleContext {
  TerminalNode? K_AS() => getToken(GremlinParser.TOKEN_K_AS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_asContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_as;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_as(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_asBoolContext extends ParserRuleContext {
  TerminalNode? K_ASBOOL() => getToken(GremlinParser.TOKEN_K_ASBOOL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asBoolContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_asBool;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asBool(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_asDateContext extends ParserRuleContext {
  TerminalNode? K_ASDATE() => getToken(GremlinParser.TOKEN_K_ASDATE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asDateContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_asDate;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asDate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_asNumberContext extends ParserRuleContext {
  TraversalMethod_asNumberContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_asNumber;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_asStringContext extends ParserRuleContext {
  TraversalMethod_asStringContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_asString;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_barrierContext extends ParserRuleContext {
  TraversalMethod_barrierContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_barrier;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_bothContext extends ParserRuleContext {
  TerminalNode? K_BOTH() => getToken(GremlinParser.TOKEN_K_BOTH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_bothContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_both;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_both(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_bothEContext extends ParserRuleContext {
  TerminalNode? K_BOTHE() => getToken(GremlinParser.TOKEN_K_BOTHE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_bothEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_bothE;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_bothE(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_bothVContext extends ParserRuleContext {
  TerminalNode? K_BOTHV() => getToken(GremlinParser.TOKEN_K_BOTHV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_bothVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_bothV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_bothV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_branchContext extends ParserRuleContext {
  TerminalNode? K_BRANCH() => getToken(GremlinParser.TOKEN_K_BRANCH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_branchContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_branch;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_branch(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_byContext extends ParserRuleContext {
  TraversalMethod_byContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_by;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_callContext extends ParserRuleContext {
  TraversalMethod_callContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_call;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_capContext extends ParserRuleContext {
  TerminalNode? K_CAP() => getToken(GremlinParser.TOKEN_K_CAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_capContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_cap;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_cap(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_chooseContext extends ParserRuleContext {
  TraversalMethod_chooseContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_choose;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_coalesceContext extends ParserRuleContext {
  TerminalNode? K_COALESCE() => getToken(GremlinParser.TOKEN_K_COALESCE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_coalesceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_coalesce;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_coalesce(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_coinContext extends ParserRuleContext {
  TerminalNode? K_COIN() => getToken(GremlinParser.TOKEN_K_COIN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NumericLiteralContext? numericLiteral() => getRuleContext<NumericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_coinContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_coin;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_coin(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_combineContext extends ParserRuleContext {
  TraversalMethod_combineContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_combine;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_concatContext extends ParserRuleContext {
  TraversalMethod_concatContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_concat;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_conjoinContext extends ParserRuleContext {
  TraversalMethod_conjoinContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_conjoin;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_connectedComponentContext extends ParserRuleContext {
  TerminalNode? K_CONNECTEDCOMPONENT() => getToken(GremlinParser.TOKEN_K_CONNECTEDCOMPONENT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_connectedComponentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_connectedComponent;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_connectedComponent(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_constantContext extends ParserRuleContext {
  TerminalNode? K_CONSTANT() => getToken(GremlinParser.TOKEN_K_CONSTANT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_constantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_constant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_constant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_countContext extends ParserRuleContext {
  TraversalMethod_countContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_count;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_cyclicPathContext extends ParserRuleContext {
  TerminalNode? K_CYCLICPATH() => getToken(GremlinParser.TOKEN_K_CYCLICPATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_cyclicPathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_cyclicPath;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_cyclicPath(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_dateAddContext extends ParserRuleContext {
  TerminalNode? K_DATEADD() => getToken(GremlinParser.TOKEN_K_DATEADD, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalDTContext? traversalDT() => getRuleContext<TraversalDTContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_dateAddContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_dateAdd;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_dateAdd(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_dateDiffContext extends ParserRuleContext {
  TraversalMethod_dateDiffContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_dateDiff;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_dedupContext extends ParserRuleContext {
  TraversalMethod_dedupContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_dedup;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_differenceContext extends ParserRuleContext {
  TraversalMethod_differenceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_difference;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_discardContext extends ParserRuleContext {
  TerminalNode? K_DISCARD() => getToken(GremlinParser.TOKEN_K_DISCARD, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_discardContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_discard;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_discard(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_disjunctContext extends ParserRuleContext {
  TraversalMethod_disjunctContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_disjunct;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_dropContext extends ParserRuleContext {
  TerminalNode? K_DROP() => getToken(GremlinParser.TOKEN_K_DROP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_dropContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_drop;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_drop(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_elementContext extends ParserRuleContext {
  TerminalNode? K_ELEMENT() => getToken(GremlinParser.TOKEN_K_ELEMENT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_elementContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_element;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_element(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_elementMapContext extends ParserRuleContext {
  TerminalNode? K_ELEMENTMAP() => getToken(GremlinParser.TOKEN_K_ELEMENTMAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_elementMapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_elementMap;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_elementMap(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_emitContext extends ParserRuleContext {
  TraversalMethod_emitContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_emit;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_failContext extends ParserRuleContext {
  TraversalMethod_failContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_fail;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_filterContext extends ParserRuleContext {
  TraversalMethod_filterContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_filter;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_flatMapContext extends ParserRuleContext {
  TerminalNode? K_FLATMAP() => getToken(GremlinParser.TOKEN_K_FLATMAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_flatMapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_flatMap;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_flatMap(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_foldContext extends ParserRuleContext {
  TraversalMethod_foldContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_fold;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_formatContext extends ParserRuleContext {
  TraversalMethod_formatContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_format;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_fromContext extends ParserRuleContext {
  TraversalMethod_fromContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_from;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_groupContext extends ParserRuleContext {
  TraversalMethod_groupContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_group;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_groupCountContext extends ParserRuleContext {
  TraversalMethod_groupCountContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_groupCount;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_hasContext extends ParserRuleContext {
  TraversalMethod_hasContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_has;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_hasIdContext extends ParserRuleContext {
  TraversalMethod_hasIdContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_hasId;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_hasKeyContext extends ParserRuleContext {
  TraversalMethod_hasKeyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_hasKey;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_hasLabelContext extends ParserRuleContext {
  TraversalMethod_hasLabelContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_hasLabel;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_hasNotContext extends ParserRuleContext {
  TerminalNode? K_HASNOT() => getToken(GremlinParser.TOKEN_K_HASNOT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_hasNotContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_hasNot;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasNot(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_hasValueContext extends ParserRuleContext {
  TraversalMethod_hasValueContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_hasValue;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_idContext extends ParserRuleContext {
  TerminalNode? K_ID() => getToken(GremlinParser.TOKEN_K_ID, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_idContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_id;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_id(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_identityContext extends ParserRuleContext {
  TerminalNode? K_IDENTITY() => getToken(GremlinParser.TOKEN_K_IDENTITY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_identityContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_identity;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_identity(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_inContext extends ParserRuleContext {
  TerminalNode? K_IN() => getToken(GremlinParser.TOKEN_K_IN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_inContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_in;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_in(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_inEContext extends ParserRuleContext {
  TerminalNode? K_INE() => getToken(GremlinParser.TOKEN_K_INE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_inEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_inE;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_inE(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_intersectContext extends ParserRuleContext {
  TraversalMethod_intersectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_intersect;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_inVContext extends ParserRuleContext {
  TerminalNode? K_INV() => getToken(GremlinParser.TOKEN_K_INV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_inVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_inV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_inV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_indexContext extends ParserRuleContext {
  TerminalNode? K_INDEX() => getToken(GremlinParser.TOKEN_K_INDEX, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_indexContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_index;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_index(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_injectContext extends ParserRuleContext {
  TerminalNode? K_INJECT() => getToken(GremlinParser.TOKEN_K_INJECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralVarargsContext? genericLiteralVarargs() => getRuleContext<GenericLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_injectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_inject;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_inject(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_isContext extends ParserRuleContext {
  TraversalMethod_isContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_is;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_keyContext extends ParserRuleContext {
  TerminalNode? K_KEY() => getToken(GremlinParser.TOKEN_K_KEY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_keyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_key;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_key(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_labelContext extends ParserRuleContext {
  TerminalNode? K_LABEL() => getToken(GremlinParser.TOKEN_K_LABEL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_labelContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_label;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_label(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_lengthContext extends ParserRuleContext {
  TraversalMethod_lengthContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_length;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_limitContext extends ParserRuleContext {
  TraversalMethod_limitContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_limit;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_localContext extends ParserRuleContext {
  TerminalNode? K_LOCAL() => getToken(GremlinParser.TOKEN_K_LOCAL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_localContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_local;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_local(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_loopsContext extends ParserRuleContext {
  TraversalMethod_loopsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_loops;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_lTrimContext extends ParserRuleContext {
  TraversalMethod_lTrimContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_lTrim;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_mapContext extends ParserRuleContext {
  TerminalNode? K_MAP() => getToken(GremlinParser.TOKEN_K_MAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_map;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_matchContext extends ParserRuleContext {
  TerminalNode? K_MATCH() => getToken(GremlinParser.TOKEN_K_MATCH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_matchContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_match;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_match(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mathContext extends ParserRuleContext {
  TerminalNode? K_MATH() => getToken(GremlinParser.TOKEN_K_MATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_math;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_math(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_maxContext extends ParserRuleContext {
  TraversalMethod_maxContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_max;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_meanContext extends ParserRuleContext {
  TraversalMethod_meanContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_mean;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_mergeContext extends ParserRuleContext {
  TraversalMethod_mergeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_merge;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_mergeVContext extends ParserRuleContext {
  TraversalMethod_mergeVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_mergeV;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_mergeEContext extends ParserRuleContext {
  TraversalMethod_mergeEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_mergeE;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_minContext extends ParserRuleContext {
  TraversalMethod_minContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_min;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_noneContext extends ParserRuleContext {
  TraversalMethod_noneContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_none;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_notContext extends ParserRuleContext {
  TerminalNode? K_NOT() => getToken(GremlinParser.TOKEN_K_NOT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_notContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_not;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_not(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_optionContext extends ParserRuleContext {
  TraversalMethod_optionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_option;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_optionalContext extends ParserRuleContext {
  TerminalNode? K_OPTIONAL() => getToken(GremlinParser.TOKEN_K_OPTIONAL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_optionalContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_optional;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_optional(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_orContext extends ParserRuleContext {
  TerminalNode? K_OR() => getToken(GremlinParser.TOKEN_K_OR, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_orContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_or;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_or(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_orderContext extends ParserRuleContext {
  TraversalMethod_orderContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_order;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_otherVContext extends ParserRuleContext {
  TerminalNode? K_OTHERV() => getToken(GremlinParser.TOKEN_K_OTHERV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_otherVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_otherV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_otherV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_outContext extends ParserRuleContext {
  TerminalNode? K_OUT() => getToken(GremlinParser.TOKEN_K_OUT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_outContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_out;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_out(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_outEContext extends ParserRuleContext {
  TerminalNode? K_OUTE() => getToken(GremlinParser.TOKEN_K_OUTE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_outEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_outE;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_outE(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_outVContext extends ParserRuleContext {
  TerminalNode? K_OUTV() => getToken(GremlinParser.TOKEN_K_OUTV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_outVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_outV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_outV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_pageRankContext extends ParserRuleContext {
  TraversalMethod_pageRankContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_pageRank;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_pathContext extends ParserRuleContext {
  TerminalNode? K_PATH() => getToken(GremlinParser.TOKEN_K_PATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_pathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_path;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_path(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_peerPressureContext extends ParserRuleContext {
  TerminalNode? K_PEERPRESSURE() => getToken(GremlinParser.TOKEN_K_PEERPRESSURE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_peerPressureContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_peerPressure;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_peerPressure(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_productContext extends ParserRuleContext {
  TraversalMethod_productContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_product;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_profileContext extends ParserRuleContext {
  TraversalMethod_profileContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_profile;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_projectContext extends ParserRuleContext {
  TerminalNode? K_PROJECT() => getToken(GremlinParser.TOKEN_K_PROJECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_projectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_project;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_project(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_propertiesContext extends ParserRuleContext {
  TerminalNode? K_PROPERTIES() => getToken(GremlinParser.TOKEN_K_PROPERTIES, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_propertiesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_properties;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_properties(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_propertyContext extends ParserRuleContext {
  TraversalMethod_propertyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_property;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_propertyMapContext extends ParserRuleContext {
  TerminalNode? K_PROPERTYMAP() => getToken(GremlinParser.TOKEN_K_PROPERTYMAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_propertyMapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_propertyMap;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_propertyMap(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_rangeContext extends ParserRuleContext {
  TraversalMethod_rangeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_range;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_readContext extends ParserRuleContext {
  TerminalNode? K_READ() => getToken(GremlinParser.TOKEN_K_READ, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_readContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_read;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_read(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_repeatContext extends ParserRuleContext {
  TraversalMethod_repeatContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_repeat;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_replaceContext extends ParserRuleContext {
  TraversalMethod_replaceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_replace;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_reverseContext extends ParserRuleContext {
  TraversalMethod_reverseContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_reverse;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_rTrimContext extends ParserRuleContext {
  TraversalMethod_rTrimContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_rTrim;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_sackContext extends ParserRuleContext {
  TraversalMethod_sackContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_sack;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_sampleContext extends ParserRuleContext {
  TraversalMethod_sampleContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_sample;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_selectContext extends ParserRuleContext {
  TraversalMethod_selectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_select;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_shortestPathContext extends ParserRuleContext {
  TerminalNode? K_SHORTESTPATH() => getToken(GremlinParser.TOKEN_K_SHORTESTPATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_shortestPathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_shortestPath;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_shortestPath(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_sideEffectContext extends ParserRuleContext {
  TerminalNode? K_SIDEEFFECT() => getToken(GremlinParser.TOKEN_K_SIDEEFFECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sideEffectContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_sideEffect;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sideEffect(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_simplePathContext extends ParserRuleContext {
  TerminalNode? K_SIMPLEPATH() => getToken(GremlinParser.TOKEN_K_SIMPLEPATH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_simplePathContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_simplePath;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_simplePath(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_skipContext extends ParserRuleContext {
  TraversalMethod_skipContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_skip;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_splitContext extends ParserRuleContext {
  TraversalMethod_splitContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_split;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_subgraphContext extends ParserRuleContext {
  TerminalNode? K_SUBGRAPH() => getToken(GremlinParser.TOKEN_K_SUBGRAPH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_subgraphContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_subgraph;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_subgraph(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_substringContext extends ParserRuleContext {
  TraversalMethod_substringContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_substring;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_sumContext extends ParserRuleContext {
  TraversalMethod_sumContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_sum;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_tailContext extends ParserRuleContext {
  TraversalMethod_tailContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_tail;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_timeLimitContext extends ParserRuleContext {
  TerminalNode? K_TIMELIMIT() => getToken(GremlinParser.TOKEN_K_TIMELIMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_timeLimitContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_timeLimit;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_timeLimit(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_timesContext extends ParserRuleContext {
  TerminalNode? K_TIMES() => getToken(GremlinParser.TOKEN_K_TIMES, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_timesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_times;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_times(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_toContext extends ParserRuleContext {
  TraversalMethod_toContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_to;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_toEContext extends ParserRuleContext {
  TerminalNode? K_TOE() => getToken(GremlinParser.TOKEN_K_TOE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalDirectionContext? traversalDirection() => getRuleContext<TraversalDirectionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TraversalMethod_toEContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_toE;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toE(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_toLowerContext extends ParserRuleContext {
  TraversalMethod_toLowerContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_toLower;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_toUpperContext extends ParserRuleContext {
  TraversalMethod_toUpperContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_toUpper;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_toVContext extends ParserRuleContext {
  TerminalNode? K_TOV() => getToken(GremlinParser.TOKEN_K_TOV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalDirectionContext? traversalDirection() => getRuleContext<TraversalDirectionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_toVContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_toV;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toV(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_treeContext extends ParserRuleContext {
  TraversalMethod_treeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_tree;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_trimContext extends ParserRuleContext {
  TraversalMethod_trimContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_trim;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_unfoldContext extends ParserRuleContext {
  TerminalNode? K_UNFOLD() => getToken(GremlinParser.TOKEN_K_UNFOLD, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_unfoldContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_unfold;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_unfold(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_unionContext extends ParserRuleContext {
  TerminalNode? K_UNION() => getToken(GremlinParser.TOKEN_K_UNION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_unionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_union;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_union(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_untilContext extends ParserRuleContext {
  TraversalMethod_untilContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_until;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_valueContext extends ParserRuleContext {
  TerminalNode? K_VALUE() => getToken(GremlinParser.TOKEN_K_VALUE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_valueContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_value;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_value(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_valueMapContext extends ParserRuleContext {
  TraversalMethod_valueMapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_valueMap;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_valuesContext extends ParserRuleContext {
  TerminalNode? K_VALUES() => getToken(GremlinParser.TOKEN_K_VALUES, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_valuesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_values;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_values(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_whereContext extends ParserRuleContext {
  TraversalMethod_whereContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_where;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_withContext extends ParserRuleContext {
  TraversalMethod_withContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_with;
 
  @override
  void copyFrom(ParserRuleContext ctx) {
    super.copyFrom(ctx);
  }
}

class TraversalMethod_writeContext extends ParserRuleContext {
  TerminalNode? K_WRITE() => getToken(GremlinParser.TOKEN_K_WRITE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_writeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMethod_write;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_write(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalStrategyContext extends ParserRuleContext {
  ClassTypeContext? classType() => getRuleContext<ClassTypeContext>(0);
  TerminalNode? K_NEW() => getToken(GremlinParser.TOKEN_K_NEW, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  List<ConfigurationContext> configurations() => getRuleContexts<ConfigurationContext>();
  ConfigurationContext? configuration(int i) => getRuleContext<ConfigurationContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  TraversalStrategyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalStrategy;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalStrategy(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConfigurationContext extends ParserRuleContext {
  TerminalNode? COLON() => getToken(GremlinParser.TOKEN_COLON, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  KeywordContext? keyword() => getRuleContext<KeywordContext>(0);
  NakedKeyContext? nakedKey() => getRuleContext<NakedKeyContext>(0);
  ConfigurationContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_configuration;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConfiguration(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalScopeContext extends ParserRuleContext {
  TerminalNode? K_LOCAL() => getToken(GremlinParser.TOKEN_K_LOCAL, 0);
  TerminalNode? K_SCOPE() => getToken(GremlinParser.TOKEN_K_SCOPE, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GLOBAL() => getToken(GremlinParser.TOKEN_K_GLOBAL, 0);
  TraversalScopeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalScope;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalScope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalBarrierContext extends ParserRuleContext {
  TerminalNode? K_NORMSACK() => getToken(GremlinParser.TOKEN_K_NORMSACK, 0);
  TerminalNode? K_BARRIERU() => getToken(GremlinParser.TOKEN_K_BARRIERU, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TraversalBarrierContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalBarrier;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalBarrier(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTContext extends ParserRuleContext {
  TraversalTShortContext? traversalTShort() => getRuleContext<TraversalTShortContext>(0);
  TraversalTLongContext? traversalTLong() => getRuleContext<TraversalTLongContext>(0);
  TraversalTContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalT;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalT(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTShortContext extends ParserRuleContext {
  TerminalNode? K_ID() => getToken(GremlinParser.TOKEN_K_ID, 0);
  TerminalNode? K_LABEL() => getToken(GremlinParser.TOKEN_K_LABEL, 0);
  TerminalNode? K_KEY() => getToken(GremlinParser.TOKEN_K_KEY, 0);
  TerminalNode? K_VALUE() => getToken(GremlinParser.TOKEN_K_VALUE, 0);
  TraversalTShortContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTShort;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTShort(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTLongContext extends ParserRuleContext {
  TerminalNode? K_T() => getToken(GremlinParser.TOKEN_K_T, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_ID() => getToken(GremlinParser.TOKEN_K_ID, 0);
  TerminalNode? K_LABEL() => getToken(GremlinParser.TOKEN_K_LABEL, 0);
  TerminalNode? K_KEY() => getToken(GremlinParser.TOKEN_K_KEY, 0);
  TerminalNode? K_VALUE() => getToken(GremlinParser.TOKEN_K_VALUE, 0);
  TraversalTLongContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTLong;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTLong(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMergeContext extends ParserRuleContext {
  TerminalNode? K_ONCREATE() => getToken(GremlinParser.TOKEN_K_ONCREATE, 0);
  TerminalNode? K_MERGEU() => getToken(GremlinParser.TOKEN_K_MERGEU, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_ONMATCH() => getToken(GremlinParser.TOKEN_K_ONMATCH, 0);
  TerminalNode? K_OUTV() => getToken(GremlinParser.TOKEN_K_OUTV, 0);
  TerminalNode? K_INV() => getToken(GremlinParser.TOKEN_K_INV, 0);
  TraversalMergeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalMerge;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMerge(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalOrderContext extends ParserRuleContext {
  TerminalNode? K_ASC() => getToken(GremlinParser.TOKEN_K_ASC, 0);
  TerminalNode? K_ORDERU() => getToken(GremlinParser.TOKEN_K_ORDERU, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_DESC() => getToken(GremlinParser.TOKEN_K_DESC, 0);
  TerminalNode? K_SHUFFLE() => getToken(GremlinParser.TOKEN_K_SHUFFLE, 0);
  TraversalOrderContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalOrder;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalOrder(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalDirectionContext extends ParserRuleContext {
  TraversalDirectionShortContext? traversalDirectionShort() => getRuleContext<TraversalDirectionShortContext>(0);
  TraversalDirectionLongContext? traversalDirectionLong() => getRuleContext<TraversalDirectionLongContext>(0);
  TraversalDirectionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalDirection;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalDirection(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalDirectionShortContext extends ParserRuleContext {
  TerminalNode? K_INU() => getToken(GremlinParser.TOKEN_K_INU, 0);
  TerminalNode? K_FROM() => getToken(GremlinParser.TOKEN_K_FROM, 0);
  TerminalNode? K_OUTU() => getToken(GremlinParser.TOKEN_K_OUTU, 0);
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? K_BOTHU() => getToken(GremlinParser.TOKEN_K_BOTHU, 0);
  TraversalDirectionShortContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalDirectionShort;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalDirectionShort(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalDirectionLongContext extends ParserRuleContext {
  TerminalNode? K_DIRECTION() => getToken(GremlinParser.TOKEN_K_DIRECTION, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_INU() => getToken(GremlinParser.TOKEN_K_INU, 0);
  TerminalNode? K_FROM() => getToken(GremlinParser.TOKEN_K_FROM, 0);
  TerminalNode? K_OUTU() => getToken(GremlinParser.TOKEN_K_OUTU, 0);
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? K_BOTHU() => getToken(GremlinParser.TOKEN_K_BOTHU, 0);
  TraversalDirectionLongContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalDirectionLong;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalDirectionLong(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalCardinalityContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_CARDINALITY() => getToken(GremlinParser.TOKEN_K_CARDINALITY, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_SINGLE() => getToken(GremlinParser.TOKEN_K_SINGLE, 0);
  TerminalNode? K_SET() => getToken(GremlinParser.TOKEN_K_SET, 0);
  TerminalNode? K_LIST() => getToken(GremlinParser.TOKEN_K_LIST, 0);
  TraversalCardinalityContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalCardinality;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalCardinality(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalColumnContext extends ParserRuleContext {
  TerminalNode? K_KEYS() => getToken(GremlinParser.TOKEN_K_KEYS, 0);
  TerminalNode? K_COLUMN() => getToken(GremlinParser.TOKEN_K_COLUMN, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_VALUES() => getToken(GremlinParser.TOKEN_K_VALUES, 0);
  TraversalColumnContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalColumn;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalColumn(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPopContext extends ParserRuleContext {
  TerminalNode? K_FIRST() => getToken(GremlinParser.TOKEN_K_FIRST, 0);
  TerminalNode? K_POP() => getToken(GremlinParser.TOKEN_K_POP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_LAST() => getToken(GremlinParser.TOKEN_K_LAST, 0);
  TerminalNode? K_ALL() => getToken(GremlinParser.TOKEN_K_ALL, 0);
  TerminalNode? K_MIXED() => getToken(GremlinParser.TOKEN_K_MIXED, 0);
  TraversalPopContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPop;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPop(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalOperatorContext extends ParserRuleContext {
  TerminalNode? K_ADDALL() => getToken(GremlinParser.TOKEN_K_ADDALL, 0);
  TerminalNode? K_OPERATOR() => getToken(GremlinParser.TOKEN_K_OPERATOR, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_AND() => getToken(GremlinParser.TOKEN_K_AND, 0);
  TerminalNode? K_ASSIGN() => getToken(GremlinParser.TOKEN_K_ASSIGN, 0);
  TerminalNode? K_DIV() => getToken(GremlinParser.TOKEN_K_DIV, 0);
  TerminalNode? K_MAX() => getToken(GremlinParser.TOKEN_K_MAX, 0);
  TerminalNode? K_MIN() => getToken(GremlinParser.TOKEN_K_MIN, 0);
  TerminalNode? K_MINUS() => getToken(GremlinParser.TOKEN_K_MINUS, 0);
  TerminalNode? K_MULT() => getToken(GremlinParser.TOKEN_K_MULT, 0);
  TerminalNode? K_OR() => getToken(GremlinParser.TOKEN_K_OR, 0);
  TerminalNode? K_SUM() => getToken(GremlinParser.TOKEN_K_SUM, 0);
  TerminalNode? K_SUMLONG() => getToken(GremlinParser.TOKEN_K_SUMLONG, 0);
  TraversalOperatorContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalOperator;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalOperator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPickContext extends ParserRuleContext {
  TerminalNode? K_ANY() => getToken(GremlinParser.TOKEN_K_ANY, 0);
  TerminalNode? K_PICK() => getToken(GremlinParser.TOKEN_K_PICK, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NONE() => getToken(GremlinParser.TOKEN_K_NONE, 0);
  TerminalNode? K_UNPRODUCTIVE() => getToken(GremlinParser.TOKEN_K_UNPRODUCTIVE, 0);
  TraversalPickContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPick;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPick(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalDTContext extends ParserRuleContext {
  TerminalNode? K_SECOND() => getToken(GremlinParser.TOKEN_K_SECOND, 0);
  TerminalNode? K_DT() => getToken(GremlinParser.TOKEN_K_DT, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_MINUTE() => getToken(GremlinParser.TOKEN_K_MINUTE, 0);
  TerminalNode? K_HOUR() => getToken(GremlinParser.TOKEN_K_HOUR, 0);
  TerminalNode? K_DAY() => getToken(GremlinParser.TOKEN_K_DAY, 0);
  TraversalDTContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalDT;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalDT(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalGTypeContext extends ParserRuleContext {
  TerminalNode? K_BIGDECIMAL() => getToken(GremlinParser.TOKEN_K_BIGDECIMAL, 0);
  TerminalNode? K_GTYPE() => getToken(GremlinParser.TOKEN_K_GTYPE, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_BIGDECIMALU() => getToken(GremlinParser.TOKEN_K_BIGDECIMALU, 0);
  TerminalNode? K_BIGINT() => getToken(GremlinParser.TOKEN_K_BIGINT, 0);
  TerminalNode? K_BIGINTU() => getToken(GremlinParser.TOKEN_K_BIGINTU, 0);
  TerminalNode? K_BINARY() => getToken(GremlinParser.TOKEN_K_BINARY, 0);
  TerminalNode? K_BINARYU() => getToken(GremlinParser.TOKEN_K_BINARYU, 0);
  TerminalNode? K_BOOLEAN() => getToken(GremlinParser.TOKEN_K_BOOLEAN, 0);
  TerminalNode? K_BOOLEANU() => getToken(GremlinParser.TOKEN_K_BOOLEANU, 0);
  TerminalNode? K_BYTE() => getToken(GremlinParser.TOKEN_K_BYTE, 0);
  TerminalNode? K_BYTEU() => getToken(GremlinParser.TOKEN_K_BYTEU, 0);
  TerminalNode? K_CHAR() => getToken(GremlinParser.TOKEN_K_CHAR, 0);
  TerminalNode? K_CHARU() => getToken(GremlinParser.TOKEN_K_CHARU, 0);
  TerminalNode? K_DATETIME() => getToken(GremlinParser.TOKEN_K_DATETIME, 0);
  TerminalNode? K_DATETIMEU() => getToken(GremlinParser.TOKEN_K_DATETIMEU, 0);
  TerminalNode? K_DOUBLE() => getToken(GremlinParser.TOKEN_K_DOUBLE, 0);
  TerminalNode? K_DOUBLEU() => getToken(GremlinParser.TOKEN_K_DOUBLEU, 0);
  TerminalNode? K_DURATION() => getToken(GremlinParser.TOKEN_K_DURATION, 0);
  TerminalNode? K_DURATIONU() => getToken(GremlinParser.TOKEN_K_DURATIONU, 0);
  TerminalNode? K_EDGE() => getToken(GremlinParser.TOKEN_K_EDGE, 0);
  TerminalNode? K_EDGEU() => getToken(GremlinParser.TOKEN_K_EDGEU, 0);
  TerminalNode? K_FLOAT() => getToken(GremlinParser.TOKEN_K_FLOAT, 0);
  TerminalNode? K_FLOATU() => getToken(GremlinParser.TOKEN_K_FLOATU, 0);
  TerminalNode? K_GRAPH() => getToken(GremlinParser.TOKEN_K_GRAPH, 0);
  TerminalNode? K_GRAPHU() => getToken(GremlinParser.TOKEN_K_GRAPHU, 0);
  TerminalNode? K_INT() => getToken(GremlinParser.TOKEN_K_INT, 0);
  TerminalNode? K_INTU() => getToken(GremlinParser.TOKEN_K_INTU, 0);
  TerminalNode? K_LIST() => getToken(GremlinParser.TOKEN_K_LIST, 0);
  TerminalNode? K_LISTU() => getToken(GremlinParser.TOKEN_K_LISTU, 0);
  TerminalNode? K_LONG() => getToken(GremlinParser.TOKEN_K_LONG, 0);
  TerminalNode? K_LONGU() => getToken(GremlinParser.TOKEN_K_LONGU, 0);
  TerminalNode? K_MAP() => getToken(GremlinParser.TOKEN_K_MAP, 0);
  TerminalNode? K_MAPU() => getToken(GremlinParser.TOKEN_K_MAPU, 0);
  TerminalNode? K_NULL() => getToken(GremlinParser.TOKEN_K_NULL, 0);
  TerminalNode? K_NULLU() => getToken(GremlinParser.TOKEN_K_NULLU, 0);
  TerminalNode? K_NUMBER() => getToken(GremlinParser.TOKEN_K_NUMBER, 0);
  TerminalNode? K_NUMBERU() => getToken(GremlinParser.TOKEN_K_NUMBERU, 0);
  TerminalNode? K_PATH() => getToken(GremlinParser.TOKEN_K_PATH, 0);
  TerminalNode? K_PATHU() => getToken(GremlinParser.TOKEN_K_PATHU, 0);
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? K_PROPERTYU() => getToken(GremlinParser.TOKEN_K_PROPERTYU, 0);
  TerminalNode? K_SET() => getToken(GremlinParser.TOKEN_K_SET, 0);
  TerminalNode? K_SETU() => getToken(GremlinParser.TOKEN_K_SETU, 0);
  TerminalNode? K_SHORT() => getToken(GremlinParser.TOKEN_K_SHORT, 0);
  TerminalNode? K_SHORTU() => getToken(GremlinParser.TOKEN_K_SHORTU, 0);
  TerminalNode? K_STRING() => getToken(GremlinParser.TOKEN_K_STRING, 0);
  TerminalNode? K_STRINGU() => getToken(GremlinParser.TOKEN_K_STRINGU, 0);
  TerminalNode? K_TREE() => getToken(GremlinParser.TOKEN_K_TREE, 0);
  TerminalNode? K_TREEU() => getToken(GremlinParser.TOKEN_K_TREEU, 0);
  TerminalNode? K_UUID() => getToken(GremlinParser.TOKEN_K_UUID, 0);
  TerminalNode? K_UUIDL() => getToken(GremlinParser.TOKEN_K_UUIDL, 0);
  TerminalNode? K_VERTEX() => getToken(GremlinParser.TOKEN_K_VERTEX, 0);
  TerminalNode? K_VERTEXU() => getToken(GremlinParser.TOKEN_K_VERTEXU, 0);
  TerminalNode? K_VPROPERTY() => getToken(GremlinParser.TOKEN_K_VPROPERTY, 0);
  TerminalNode? K_VPROPERTYU() => getToken(GremlinParser.TOKEN_K_VPROPERTYU, 0);
  TraversalGTypeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalGType;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalGType(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicateContext extends ParserRuleContext {
  TraversalPredicate_eqContext? traversalPredicate_eq() => getRuleContext<TraversalPredicate_eqContext>(0);
  TraversalPredicate_neqContext? traversalPredicate_neq() => getRuleContext<TraversalPredicate_neqContext>(0);
  TraversalPredicate_ltContext? traversalPredicate_lt() => getRuleContext<TraversalPredicate_ltContext>(0);
  TraversalPredicate_lteContext? traversalPredicate_lte() => getRuleContext<TraversalPredicate_lteContext>(0);
  TraversalPredicate_gtContext? traversalPredicate_gt() => getRuleContext<TraversalPredicate_gtContext>(0);
  TraversalPredicate_gteContext? traversalPredicate_gte() => getRuleContext<TraversalPredicate_gteContext>(0);
  TraversalPredicate_insideContext? traversalPredicate_inside() => getRuleContext<TraversalPredicate_insideContext>(0);
  TraversalPredicate_outsideContext? traversalPredicate_outside() => getRuleContext<TraversalPredicate_outsideContext>(0);
  TraversalPredicate_betweenContext? traversalPredicate_between() => getRuleContext<TraversalPredicate_betweenContext>(0);
  TraversalPredicate_typeOfContext? traversalPredicate_typeOf() => getRuleContext<TraversalPredicate_typeOfContext>(0);
  TraversalPredicate_withinContext? traversalPredicate_within() => getRuleContext<TraversalPredicate_withinContext>(0);
  TraversalPredicate_withoutContext? traversalPredicate_without() => getRuleContext<TraversalPredicate_withoutContext>(0);
  TraversalPredicate_notContext? traversalPredicate_not() => getRuleContext<TraversalPredicate_notContext>(0);
  TraversalPredicate_startingWithContext? traversalPredicate_startingWith() => getRuleContext<TraversalPredicate_startingWithContext>(0);
  TraversalPredicate_notStartingWithContext? traversalPredicate_notStartingWith() => getRuleContext<TraversalPredicate_notStartingWithContext>(0);
  TraversalPredicate_endingWithContext? traversalPredicate_endingWith() => getRuleContext<TraversalPredicate_endingWithContext>(0);
  TraversalPredicate_notEndingWithContext? traversalPredicate_notEndingWith() => getRuleContext<TraversalPredicate_notEndingWithContext>(0);
  TraversalPredicate_containingContext? traversalPredicate_containing() => getRuleContext<TraversalPredicate_containingContext>(0);
  TraversalPredicate_notContainingContext? traversalPredicate_notContaining() => getRuleContext<TraversalPredicate_notContainingContext>(0);
  TraversalPredicate_regexContext? traversalPredicate_regex() => getRuleContext<TraversalPredicate_regexContext>(0);
  TraversalPredicate_notRegexContext? traversalPredicate_notRegex() => getRuleContext<TraversalPredicate_notRegexContext>(0);
  List<TraversalPredicateContext> traversalPredicates() => getRuleContexts<TraversalPredicateContext>();
  TraversalPredicateContext? traversalPredicate(int i) => getRuleContext<TraversalPredicateContext>(i);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_AND() => getToken(GremlinParser.TOKEN_K_AND, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_OR() => getToken(GremlinParser.TOKEN_K_OR, 0);
  TerminalNode? K_NEGATE() => getToken(GremlinParser.TOKEN_K_NEGATE, 0);
  TraversalPredicateContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethodContext extends ParserRuleContext {
  TraversalTerminalMethod_explainContext? traversalTerminalMethod_explain() => getRuleContext<TraversalTerminalMethod_explainContext>(0);
  TraversalTerminalMethod_iterateContext? traversalTerminalMethod_iterate() => getRuleContext<TraversalTerminalMethod_iterateContext>(0);
  TraversalTerminalMethod_hasNextContext? traversalTerminalMethod_hasNext() => getRuleContext<TraversalTerminalMethod_hasNextContext>(0);
  TraversalTerminalMethod_tryNextContext? traversalTerminalMethod_tryNext() => getRuleContext<TraversalTerminalMethod_tryNextContext>(0);
  TraversalTerminalMethod_nextContext? traversalTerminalMethod_next() => getRuleContext<TraversalTerminalMethod_nextContext>(0);
  TraversalTerminalMethod_toListContext? traversalTerminalMethod_toList() => getRuleContext<TraversalTerminalMethod_toListContext>(0);
  TraversalTerminalMethod_toSetContext? traversalTerminalMethod_toSet() => getRuleContext<TraversalTerminalMethod_toSetContext>(0);
  TraversalTerminalMethod_toBulkSetContext? traversalTerminalMethod_toBulkSet() => getRuleContext<TraversalTerminalMethod_toBulkSetContext>(0);
  TraversalTerminalMethodContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSackMethodContext extends ParserRuleContext {
  TraversalBarrierContext? traversalBarrier() => getRuleContext<TraversalBarrierContext>(0);
  TraversalSackMethodContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalSackMethod;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSackMethod(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalComparatorContext extends ParserRuleContext {
  TraversalOrderContext? traversalOrder() => getRuleContext<TraversalOrderContext>(0);
  TraversalComparatorContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalComparator;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalComparator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalFunctionContext extends ParserRuleContext {
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TraversalColumnContext? traversalColumn() => getRuleContext<TraversalColumnContext>(0);
  TraversalFunctionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalFunction;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalFunction(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalBiFunctionContext extends ParserRuleContext {
  TraversalOperatorContext? traversalOperator() => getRuleContext<TraversalOperatorContext>(0);
  TraversalBiFunctionContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalBiFunction;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalBiFunction(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_eqContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_EQ() => getToken(GremlinParser.TOKEN_K_EQ, 0);
  TraversalPredicate_eqContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_eq;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_eq(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_neqContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NEQ() => getToken(GremlinParser.TOKEN_K_NEQ, 0);
  TraversalPredicate_neqContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_neq;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_neq(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_typeOfContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalGTypeContext? traversalGType() => getRuleContext<TraversalGTypeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_TYPEOF() => getToken(GremlinParser.TOKEN_K_TYPEOF, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TraversalPredicate_typeOfContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_typeOf;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_typeOf(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_ltContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_LT() => getToken(GremlinParser.TOKEN_K_LT, 0);
  TraversalPredicate_ltContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_lt;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_lt(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_lteContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_LTE() => getToken(GremlinParser.TOKEN_K_LTE, 0);
  TraversalPredicate_lteContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_lte;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_lte(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_gtContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GT() => getToken(GremlinParser.TOKEN_K_GT, 0);
  TraversalPredicate_gtContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_gt;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_gt(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_gteContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GTE() => getToken(GremlinParser.TOKEN_K_GTE, 0);
  TraversalPredicate_gteContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_gte;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_gte(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_insideContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<GenericArgumentContext> genericArguments() => getRuleContexts<GenericArgumentContext>();
  GenericArgumentContext? genericArgument(int i) => getRuleContext<GenericArgumentContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_INSIDE() => getToken(GremlinParser.TOKEN_K_INSIDE, 0);
  TraversalPredicate_insideContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_inside;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_inside(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_outsideContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<GenericArgumentContext> genericArguments() => getRuleContexts<GenericArgumentContext>();
  GenericArgumentContext? genericArgument(int i) => getRuleContext<GenericArgumentContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_OUTSIDE() => getToken(GremlinParser.TOKEN_K_OUTSIDE, 0);
  TraversalPredicate_outsideContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_outside;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_outside(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_betweenContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<GenericArgumentContext> genericArguments() => getRuleContexts<GenericArgumentContext>();
  GenericArgumentContext? genericArgument(int i) => getRuleContext<GenericArgumentContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_BETWEEN() => getToken(GremlinParser.TOKEN_K_BETWEEN, 0);
  TraversalPredicate_betweenContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_between;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_between(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_withinContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_WITHIN() => getToken(GremlinParser.TOKEN_K_WITHIN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalPredicate_withinContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_within;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_within(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_withoutContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_WITHOUT() => getToken(GremlinParser.TOKEN_K_WITHOUT, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalPredicate_withoutContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_without;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_without(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_notContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NOT() => getToken(GremlinParser.TOKEN_K_NOT, 0);
  TraversalPredicate_notContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_not;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_not(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_containingContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_CONTAINING() => getToken(GremlinParser.TOKEN_K_CONTAINING, 0);
  TraversalPredicate_containingContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_containing;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_containing(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_notContainingContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NOTCONTAINING() => getToken(GremlinParser.TOKEN_K_NOTCONTAINING, 0);
  TraversalPredicate_notContainingContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_notContaining;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_notContaining(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_startingWithContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_STARTINGWITH() => getToken(GremlinParser.TOKEN_K_STARTINGWITH, 0);
  TraversalPredicate_startingWithContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_startingWith;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_startingWith(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_notStartingWithContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NOTSTARTINGWITH() => getToken(GremlinParser.TOKEN_K_NOTSTARTINGWITH, 0);
  TraversalPredicate_notStartingWithContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_notStartingWith;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_notStartingWith(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_endingWithContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_ENDINGWITH() => getToken(GremlinParser.TOKEN_K_ENDINGWITH, 0);
  TraversalPredicate_endingWithContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_endingWith;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_endingWith(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_notEndingWithContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NOTENDINGWITH() => getToken(GremlinParser.TOKEN_K_NOTENDINGWITH, 0);
  TraversalPredicate_notEndingWithContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_notEndingWith;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_notEndingWith(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_regexContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_REGEX() => getToken(GremlinParser.TOKEN_K_REGEX, 0);
  TraversalPredicate_regexContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_regex;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_regex(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalPredicate_notRegexContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NOTREGEX() => getToken(GremlinParser.TOKEN_K_NOTREGEX, 0);
  TraversalPredicate_notRegexContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalPredicate_notRegex;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalPredicate_notRegex(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_explainContext extends ParserRuleContext {
  TerminalNode? K_EXPLAIN() => getToken(GremlinParser.TOKEN_K_EXPLAIN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_explainContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_explain;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_explain(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_hasNextContext extends ParserRuleContext {
  TerminalNode? K_HASNEXT() => getToken(GremlinParser.TOKEN_K_HASNEXT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_hasNextContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_hasNext;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_hasNext(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_iterateContext extends ParserRuleContext {
  TerminalNode? K_ITERATE() => getToken(GremlinParser.TOKEN_K_ITERATE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_iterateContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_iterate;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_iterate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_tryNextContext extends ParserRuleContext {
  TerminalNode? K_TRYNEXT() => getToken(GremlinParser.TOKEN_K_TRYNEXT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_tryNextContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_tryNext;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_tryNext(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_nextContext extends ParserRuleContext {
  TerminalNode? K_NEXT() => getToken(GremlinParser.TOKEN_K_NEXT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TraversalTerminalMethod_nextContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_next;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_next(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_toListContext extends ParserRuleContext {
  TerminalNode? K_TOLIST() => getToken(GremlinParser.TOKEN_K_TOLIST, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_toListContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_toList;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_toList(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_toSetContext extends ParserRuleContext {
  TerminalNode? K_TOSET() => getToken(GremlinParser.TOKEN_K_TOSET, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_toSetContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_toSet;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_toSet(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalTerminalMethod_toBulkSetContext extends ParserRuleContext {
  TerminalNode? K_TOBULKSET() => getToken(GremlinParser.TOKEN_K_TOBULKSET, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTerminalMethod_toBulkSetContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalTerminalMethod_toBulkSet;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalTerminalMethod_toBulkSet(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionKeysContext extends ParserRuleContext {
  ShortestPathConstantsContext? shortestPathConstants() => getRuleContext<ShortestPathConstantsContext>(0);
  ConnectedComponentConstantsContext? connectedComponentConstants() => getRuleContext<ConnectedComponentConstantsContext>(0);
  PageRankConstantsContext? pageRankConstants() => getRuleContext<PageRankConstantsContext>(0);
  PeerPressureConstantsContext? peerPressureConstants() => getRuleContext<PeerPressureConstantsContext>(0);
  IoOptionsKeysContext? ioOptionsKeys() => getRuleContext<IoOptionsKeysContext>(0);
  WithOptionsConstants_tokensContext? withOptionsConstants_tokens() => getRuleContext<WithOptionsConstants_tokensContext>(0);
  WithOptionsConstants_indexerContext? withOptionsConstants_indexer() => getRuleContext<WithOptionsConstants_indexerContext>(0);
  WithOptionKeysContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionKeys;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionKeys(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConnectedComponentConstantsContext extends ParserRuleContext {
  ConnectedComponentConstants_componentContext? connectedComponentConstants_component() => getRuleContext<ConnectedComponentConstants_componentContext>(0);
  ConnectedComponentConstants_edgesContext? connectedComponentConstants_edges() => getRuleContext<ConnectedComponentConstants_edgesContext>(0);
  ConnectedComponentConstants_propertyNameContext? connectedComponentConstants_propertyName() => getRuleContext<ConnectedComponentConstants_propertyNameContext>(0);
  ConnectedComponentConstantsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_connectedComponentConstants;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConnectedComponentConstants(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PageRankConstantsContext extends ParserRuleContext {
  PageRankConstants_edgesContext? pageRankConstants_edges() => getRuleContext<PageRankConstants_edgesContext>(0);
  PageRankConstants_timesContext? pageRankConstants_times() => getRuleContext<PageRankConstants_timesContext>(0);
  PageRankConstants_propertyNameContext? pageRankConstants_propertyName() => getRuleContext<PageRankConstants_propertyNameContext>(0);
  PageRankConstantsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_pageRankConstants;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPageRankConstants(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PeerPressureConstantsContext extends ParserRuleContext {
  PeerPressureConstants_edgesContext? peerPressureConstants_edges() => getRuleContext<PeerPressureConstants_edgesContext>(0);
  PeerPressureConstants_timesContext? peerPressureConstants_times() => getRuleContext<PeerPressureConstants_timesContext>(0);
  PeerPressureConstants_propertyNameContext? peerPressureConstants_propertyName() => getRuleContext<PeerPressureConstants_propertyNameContext>(0);
  PeerPressureConstantsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_peerPressureConstants;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPeerPressureConstants(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstantsContext extends ParserRuleContext {
  ShortestPathConstants_targetContext? shortestPathConstants_target() => getRuleContext<ShortestPathConstants_targetContext>(0);
  ShortestPathConstants_edgesContext? shortestPathConstants_edges() => getRuleContext<ShortestPathConstants_edgesContext>(0);
  ShortestPathConstants_distanceContext? shortestPathConstants_distance() => getRuleContext<ShortestPathConstants_distanceContext>(0);
  ShortestPathConstants_maxDistanceContext? shortestPathConstants_maxDistance() => getRuleContext<ShortestPathConstants_maxDistanceContext>(0);
  ShortestPathConstants_includeEdgesContext? shortestPathConstants_includeEdges() => getRuleContext<ShortestPathConstants_includeEdgesContext>(0);
  ShortestPathConstantsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsValuesContext extends ParserRuleContext {
  WithOptionsConstants_tokensContext? withOptionsConstants_tokens() => getRuleContext<WithOptionsConstants_tokensContext>(0);
  WithOptionsConstants_noneContext? withOptionsConstants_none() => getRuleContext<WithOptionsConstants_noneContext>(0);
  WithOptionsConstants_idsContext? withOptionsConstants_ids() => getRuleContext<WithOptionsConstants_idsContext>(0);
  WithOptionsConstants_labelsContext? withOptionsConstants_labels() => getRuleContext<WithOptionsConstants_labelsContext>(0);
  WithOptionsConstants_keysContext? withOptionsConstants_keys() => getRuleContext<WithOptionsConstants_keysContext>(0);
  WithOptionsConstants_valuesContext? withOptionsConstants_values() => getRuleContext<WithOptionsConstants_valuesContext>(0);
  WithOptionsConstants_allContext? withOptionsConstants_all() => getRuleContext<WithOptionsConstants_allContext>(0);
  WithOptionsConstants_listContext? withOptionsConstants_list() => getRuleContext<WithOptionsConstants_listContext>(0);
  WithOptionsConstants_mapContext? withOptionsConstants_map() => getRuleContext<WithOptionsConstants_mapContext>(0);
  WithOptionsValuesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsValues;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsValues(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsKeysContext extends ParserRuleContext {
  IoOptionsConstants_readerContext? ioOptionsConstants_reader() => getRuleContext<IoOptionsConstants_readerContext>(0);
  IoOptionsConstants_writerContext? ioOptionsConstants_writer() => getRuleContext<IoOptionsConstants_writerContext>(0);
  IoOptionsKeysContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsKeys;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsKeys(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsValuesContext extends ParserRuleContext {
  IoOptionsConstants_gryoContext? ioOptionsConstants_gryo() => getRuleContext<IoOptionsConstants_gryoContext>(0);
  IoOptionsConstants_graphsonContext? ioOptionsConstants_graphson() => getRuleContext<IoOptionsConstants_graphsonContext>(0);
  IoOptionsConstants_graphmlContext? ioOptionsConstants_graphml() => getRuleContext<IoOptionsConstants_graphmlContext>(0);
  IoOptionsValuesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsValues;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsValues(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConnectedComponentConstants_componentContext extends ParserRuleContext {
  ConnectedComponentStringConstantContext? connectedComponentStringConstant() => getRuleContext<ConnectedComponentStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_COMPONENT() => getToken(GremlinParser.TOKEN_K_COMPONENT, 0);
  ConnectedComponentConstants_componentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_connectedComponentConstants_component;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConnectedComponentConstants_component(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConnectedComponentConstants_edgesContext extends ParserRuleContext {
  ConnectedComponentStringConstantContext? connectedComponentStringConstant() => getRuleContext<ConnectedComponentStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_EDGES() => getToken(GremlinParser.TOKEN_K_EDGES, 0);
  ConnectedComponentConstants_edgesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_connectedComponentConstants_edges;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConnectedComponentConstants_edges(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConnectedComponentConstants_propertyNameContext extends ParserRuleContext {
  ConnectedComponentStringConstantContext? connectedComponentStringConstant() => getRuleContext<ConnectedComponentStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_PROPERTYNAME() => getToken(GremlinParser.TOKEN_K_PROPERTYNAME, 0);
  ConnectedComponentConstants_propertyNameContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_connectedComponentConstants_propertyName;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConnectedComponentConstants_propertyName(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PageRankConstants_edgesContext extends ParserRuleContext {
  PageRankStringConstantContext? pageRankStringConstant() => getRuleContext<PageRankStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_EDGES() => getToken(GremlinParser.TOKEN_K_EDGES, 0);
  PageRankConstants_edgesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_pageRankConstants_edges;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPageRankConstants_edges(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PageRankConstants_timesContext extends ParserRuleContext {
  PageRankStringConstantContext? pageRankStringConstant() => getRuleContext<PageRankStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_TIMES() => getToken(GremlinParser.TOKEN_K_TIMES, 0);
  PageRankConstants_timesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_pageRankConstants_times;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPageRankConstants_times(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PageRankConstants_propertyNameContext extends ParserRuleContext {
  PageRankStringConstantContext? pageRankStringConstant() => getRuleContext<PageRankStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_PROPERTYNAME() => getToken(GremlinParser.TOKEN_K_PROPERTYNAME, 0);
  PageRankConstants_propertyNameContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_pageRankConstants_propertyName;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPageRankConstants_propertyName(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PeerPressureConstants_edgesContext extends ParserRuleContext {
  PeerPressureStringConstantContext? peerPressureStringConstant() => getRuleContext<PeerPressureStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_EDGES() => getToken(GremlinParser.TOKEN_K_EDGES, 0);
  PeerPressureConstants_edgesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_peerPressureConstants_edges;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPeerPressureConstants_edges(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PeerPressureConstants_timesContext extends ParserRuleContext {
  PeerPressureStringConstantContext? peerPressureStringConstant() => getRuleContext<PeerPressureStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_TIMES() => getToken(GremlinParser.TOKEN_K_TIMES, 0);
  PeerPressureConstants_timesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_peerPressureConstants_times;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPeerPressureConstants_times(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PeerPressureConstants_propertyNameContext extends ParserRuleContext {
  PeerPressureStringConstantContext? peerPressureStringConstant() => getRuleContext<PeerPressureStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_PROPERTYNAME() => getToken(GremlinParser.TOKEN_K_PROPERTYNAME, 0);
  PeerPressureConstants_propertyNameContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_peerPressureConstants_propertyName;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPeerPressureConstants_propertyName(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstants_targetContext extends ParserRuleContext {
  ShortestPathStringConstantContext? shortestPathStringConstant() => getRuleContext<ShortestPathStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_TARGET() => getToken(GremlinParser.TOKEN_K_TARGET, 0);
  ShortestPathConstants_targetContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants_target;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants_target(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstants_edgesContext extends ParserRuleContext {
  ShortestPathStringConstantContext? shortestPathStringConstant() => getRuleContext<ShortestPathStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_EDGES() => getToken(GremlinParser.TOKEN_K_EDGES, 0);
  ShortestPathConstants_edgesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants_edges;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants_edges(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstants_distanceContext extends ParserRuleContext {
  ShortestPathStringConstantContext? shortestPathStringConstant() => getRuleContext<ShortestPathStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_DISTANCE() => getToken(GremlinParser.TOKEN_K_DISTANCE, 0);
  ShortestPathConstants_distanceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants_distance;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants_distance(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstants_maxDistanceContext extends ParserRuleContext {
  ShortestPathStringConstantContext? shortestPathStringConstant() => getRuleContext<ShortestPathStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_MAXDISTANCE() => getToken(GremlinParser.TOKEN_K_MAXDISTANCE, 0);
  ShortestPathConstants_maxDistanceContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants_maxDistance;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants_maxDistance(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathConstants_includeEdgesContext extends ParserRuleContext {
  ShortestPathStringConstantContext? shortestPathStringConstant() => getRuleContext<ShortestPathStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_INCLUDEEDGES() => getToken(GremlinParser.TOKEN_K_INCLUDEEDGES, 0);
  ShortestPathConstants_includeEdgesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathConstants_includeEdges;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathConstants_includeEdges(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_tokensContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_TOKENS() => getToken(GremlinParser.TOKEN_K_TOKENS, 0);
  WithOptionsConstants_tokensContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_tokens;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_tokens(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_noneContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_NONE() => getToken(GremlinParser.TOKEN_K_NONE, 0);
  WithOptionsConstants_noneContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_none;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_none(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_idsContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_IDS() => getToken(GremlinParser.TOKEN_K_IDS, 0);
  WithOptionsConstants_idsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_ids;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_ids(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_labelsContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_LABELS() => getToken(GremlinParser.TOKEN_K_LABELS, 0);
  WithOptionsConstants_labelsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_labels;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_labels(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_keysContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_KEYS() => getToken(GremlinParser.TOKEN_K_KEYS, 0);
  WithOptionsConstants_keysContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_keys;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_keys(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_valuesContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_VALUES() => getToken(GremlinParser.TOKEN_K_VALUES, 0);
  WithOptionsConstants_valuesContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_values;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_values(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_allContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_ALL() => getToken(GremlinParser.TOKEN_K_ALL, 0);
  WithOptionsConstants_allContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_all;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_all(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_indexerContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_INDEXER() => getToken(GremlinParser.TOKEN_K_INDEXER, 0);
  WithOptionsConstants_indexerContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_indexer;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_indexer(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_listContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_LIST() => getToken(GremlinParser.TOKEN_K_LIST, 0);
  WithOptionsConstants_listContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_list;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_list(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsConstants_mapContext extends ParserRuleContext {
  WithOptionsStringConstantContext? withOptionsStringConstant() => getRuleContext<WithOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_MAP() => getToken(GremlinParser.TOKEN_K_MAP, 0);
  WithOptionsConstants_mapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsConstants_map;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsConstants_map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsConstants_readerContext extends ParserRuleContext {
  IoOptionsStringConstantContext? ioOptionsStringConstant() => getRuleContext<IoOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_READER() => getToken(GremlinParser.TOKEN_K_READER, 0);
  IoOptionsConstants_readerContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsConstants_reader;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsConstants_reader(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsConstants_writerContext extends ParserRuleContext {
  IoOptionsStringConstantContext? ioOptionsStringConstant() => getRuleContext<IoOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_WRITER() => getToken(GremlinParser.TOKEN_K_WRITER, 0);
  IoOptionsConstants_writerContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsConstants_writer;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsConstants_writer(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsConstants_gryoContext extends ParserRuleContext {
  IoOptionsStringConstantContext? ioOptionsStringConstant() => getRuleContext<IoOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GRYO() => getToken(GremlinParser.TOKEN_K_GRYO, 0);
  IoOptionsConstants_gryoContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsConstants_gryo;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsConstants_gryo(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsConstants_graphsonContext extends ParserRuleContext {
  IoOptionsStringConstantContext? ioOptionsStringConstant() => getRuleContext<IoOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GRAPHSON() => getToken(GremlinParser.TOKEN_K_GRAPHSON, 0);
  IoOptionsConstants_graphsonContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsConstants_graphson;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsConstants_graphson(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsConstants_graphmlContext extends ParserRuleContext {
  IoOptionsStringConstantContext? ioOptionsStringConstant() => getRuleContext<IoOptionsStringConstantContext>(0);
  TerminalNode? DOT() => getToken(GremlinParser.TOKEN_DOT, 0);
  TerminalNode? K_GRAPHML() => getToken(GremlinParser.TOKEN_K_GRAPHML, 0);
  IoOptionsConstants_graphmlContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsConstants_graphml;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsConstants_graphml(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ConnectedComponentStringConstantContext extends ParserRuleContext {
  TerminalNode? K_CONNECTEDCOMPONENTU() => getToken(GremlinParser.TOKEN_K_CONNECTEDCOMPONENTU, 0);
  ConnectedComponentStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_connectedComponentStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitConnectedComponentStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PageRankStringConstantContext extends ParserRuleContext {
  TerminalNode? K_PAGERANKU() => getToken(GremlinParser.TOKEN_K_PAGERANKU, 0);
  PageRankStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_pageRankStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPageRankStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class PeerPressureStringConstantContext extends ParserRuleContext {
  TerminalNode? K_PEERPRESSUREU() => getToken(GremlinParser.TOKEN_K_PEERPRESSUREU, 0);
  PeerPressureStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_peerPressureStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitPeerPressureStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ShortestPathStringConstantContext extends ParserRuleContext {
  TerminalNode? K_SHORTESTPATHU() => getToken(GremlinParser.TOKEN_K_SHORTESTPATHU, 0);
  ShortestPathStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_shortestPathStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitShortestPathStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class WithOptionsStringConstantContext extends ParserRuleContext {
  TerminalNode? K_WITHOPTOPTIONS() => getToken(GremlinParser.TOKEN_K_WITHOPTOPTIONS, 0);
  WithOptionsStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_withOptionsStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitWithOptionsStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IoOptionsStringConstantContext extends ParserRuleContext {
  TerminalNode? K_IOU() => getToken(GremlinParser.TOKEN_K_IOU, 0);
  IoOptionsStringConstantContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_ioOptionsStringConstant;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIoOptionsStringConstant(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class BooleanArgumentContext extends ParserRuleContext {
  BooleanLiteralContext? booleanLiteral() => getRuleContext<BooleanLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  BooleanArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_booleanArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitBooleanArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IntegerArgumentContext extends ParserRuleContext {
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  IntegerArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_integerArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIntegerArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringArgumentContext extends ParserRuleContext {
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  StringArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringNullableArgumentContext extends ParserRuleContext {
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  StringNullableArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringNullableArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringNullableArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringNullableArgumentVarargsContext extends ParserRuleContext {
  List<StringNullableArgumentContext> stringNullableArguments() => getRuleContexts<StringNullableArgumentContext>();
  StringNullableArgumentContext? stringNullableArgument(int i) => getRuleContext<StringNullableArgumentContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  StringNullableArgumentVarargsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringNullableArgumentVarargs;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringNullableArgumentVarargs(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class DateArgumentContext extends ParserRuleContext {
  DateLiteralContext? dateLiteral() => getRuleContext<DateLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  DateArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_dateArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitDateArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericArgumentContext extends ParserRuleContext {
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  GenericArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericArgumentVarargsContext extends ParserRuleContext {
  List<GenericArgumentContext> genericArguments() => getRuleContexts<GenericArgumentContext>();
  GenericArgumentContext? genericArgument(int i) => getRuleContext<GenericArgumentContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericArgumentVarargsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericArgumentVarargs;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericArgumentVarargs(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericMapArgumentContext extends ParserRuleContext {
  GenericMapLiteralContext? genericMapLiteral() => getRuleContext<GenericMapLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  GenericMapArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericMapArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericMapArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericMapNullableArgumentContext extends ParserRuleContext {
  GenericMapNullableLiteralContext? genericMapNullableLiteral() => getRuleContext<GenericMapNullableLiteralContext>(0);
  VariableContext? variable() => getRuleContext<VariableContext>(0);
  GenericMapNullableArgumentContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericMapNullableArgument;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericMapNullableArgument(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NullableGenericLiteralMapContext extends ParserRuleContext {
  GenericMapLiteralContext? genericMapLiteral() => getRuleContext<GenericMapLiteralContext>(0);
  NullLiteralContext? nullLiteral() => getRuleContext<NullLiteralContext>(0);
  NullableGenericLiteralMapContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nullableGenericLiteralMap;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNullableGenericLiteralMap(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalStrategyVarargsContext extends ParserRuleContext {
  TraversalStrategyExprContext? traversalStrategyExpr() => getRuleContext<TraversalStrategyExprContext>(0);
  TraversalStrategyVarargsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalStrategyVarargs;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalStrategyVarargs(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalStrategyExprContext extends ParserRuleContext {
  List<TraversalStrategyContext> traversalStrategys() => getRuleContexts<TraversalStrategyContext>();
  TraversalStrategyContext? traversalStrategy(int i) => getRuleContext<TraversalStrategyContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  TraversalStrategyExprContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_traversalStrategyExpr;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalStrategyExpr(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ClassTypeListContext extends ParserRuleContext {
  ClassTypeExprContext? classTypeExpr() => getRuleContext<ClassTypeExprContext>(0);
  ClassTypeListContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_classTypeList;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitClassTypeList(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ClassTypeExprContext extends ParserRuleContext {
  List<ClassTypeContext> classTypes() => getRuleContexts<ClassTypeContext>();
  ClassTypeContext? classType(int i) => getRuleContext<ClassTypeContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  ClassTypeExprContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_classTypeExpr;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitClassTypeExpr(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NestedTraversalListContext extends ParserRuleContext {
  NestedTraversalExprContext? nestedTraversalExpr() => getRuleContext<NestedTraversalExprContext>(0);
  NestedTraversalListContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nestedTraversalList;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNestedTraversalList(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NestedTraversalExprContext extends ParserRuleContext {
  List<NestedTraversalContext> nestedTraversals() => getRuleContexts<NestedTraversalContext>();
  NestedTraversalContext? nestedTraversal(int i) => getRuleContext<NestedTraversalContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  NestedTraversalExprContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nestedTraversalExpr;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNestedTraversalExpr(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericCollectionLiteralContext extends ParserRuleContext {
  TerminalNode? LBRACK() => getToken(GremlinParser.TOKEN_LBRACK, 0);
  TerminalNode? RBRACK() => getToken(GremlinParser.TOKEN_RBRACK, 0);
  List<GenericLiteralContext> genericLiterals() => getRuleContexts<GenericLiteralContext>();
  GenericLiteralContext? genericLiteral(int i) => getRuleContext<GenericLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericCollectionLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericCollectionLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericCollectionLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericLiteralVarargsContext extends ParserRuleContext {
  GenericLiteralExprContext? genericLiteralExpr() => getRuleContext<GenericLiteralExprContext>(0);
  GenericLiteralVarargsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericLiteralVarargs;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericLiteralVarargs(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericLiteralExprContext extends ParserRuleContext {
  List<GenericLiteralContext> genericLiterals() => getRuleContexts<GenericLiteralContext>();
  GenericLiteralContext? genericLiteral(int i) => getRuleContext<GenericLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericLiteralExprContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericLiteralExpr;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericLiteralExpr(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericMapNullableLiteralContext extends ParserRuleContext {
  GenericMapLiteralContext? genericMapLiteral() => getRuleContext<GenericMapLiteralContext>(0);
  NullLiteralContext? nullLiteral() => getRuleContext<NullLiteralContext>(0);
  GenericMapNullableLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericMapNullableLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericMapNullableLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericRangeLiteralContext extends ParserRuleContext {
  List<IntegerLiteralContext> integerLiterals() => getRuleContexts<IntegerLiteralContext>();
  IntegerLiteralContext? integerLiteral(int i) => getRuleContext<IntegerLiteralContext>(i);
  List<TerminalNode> DOTs() => getTokens(GremlinParser.TOKEN_DOT);
  TerminalNode? DOT(int i) => getToken(GremlinParser.TOKEN_DOT, i);
  List<StringLiteralContext> stringLiterals() => getRuleContexts<StringLiteralContext>();
  StringLiteralContext? stringLiteral(int i) => getRuleContext<StringLiteralContext>(i);
  GenericRangeLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericRangeLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericRangeLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericSetLiteralContext extends ParserRuleContext {
  TerminalNode? LBRACE() => getToken(GremlinParser.TOKEN_LBRACE, 0);
  TerminalNode? RBRACE() => getToken(GremlinParser.TOKEN_RBRACE, 0);
  List<GenericLiteralContext> genericLiterals() => getRuleContexts<GenericLiteralContext>();
  GenericLiteralContext? genericLiteral(int i) => getRuleContext<GenericLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericSetLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericSetLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericSetLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringNullableLiteralVarargsContext extends ParserRuleContext {
  List<StringNullableLiteralContext> stringNullableLiterals() => getRuleContexts<StringNullableLiteralContext>();
  StringNullableLiteralContext? stringNullableLiteral(int i) => getRuleContext<StringNullableLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  StringNullableLiteralVarargsContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringNullableLiteralVarargs;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringNullableLiteralVarargs(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericLiteralContext extends ParserRuleContext {
  NumericLiteralContext? numericLiteral() => getRuleContext<NumericLiteralContext>(0);
  BooleanLiteralContext? booleanLiteral() => getRuleContext<BooleanLiteralContext>(0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  DateLiteralContext? dateLiteral() => getRuleContext<DateLiteralContext>(0);
  NullLiteralContext? nullLiteral() => getRuleContext<NullLiteralContext>(0);
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TraversalCardinalityContext? traversalCardinality() => getRuleContext<TraversalCardinalityContext>(0);
  TraversalDirectionContext? traversalDirection() => getRuleContext<TraversalDirectionContext>(0);
  TraversalMergeContext? traversalMerge() => getRuleContext<TraversalMergeContext>(0);
  TraversalPickContext? traversalPick() => getRuleContext<TraversalPickContext>(0);
  TraversalDTContext? traversalDT() => getRuleContext<TraversalDTContext>(0);
  TraversalGTypeContext? traversalGType() => getRuleContext<TraversalGTypeContext>(0);
  GenericSetLiteralContext? genericSetLiteral() => getRuleContext<GenericSetLiteralContext>(0);
  GenericCollectionLiteralContext? genericCollectionLiteral() => getRuleContext<GenericCollectionLiteralContext>(0);
  GenericRangeLiteralContext? genericRangeLiteral() => getRuleContext<GenericRangeLiteralContext>(0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminatedTraversalContext? terminatedTraversal() => getRuleContext<TerminatedTraversalContext>(0);
  UuidLiteralContext? uuidLiteral() => getRuleContext<UuidLiteralContext>(0);
  CharacterLiteralContext? characterLiteral() => getRuleContext<CharacterLiteralContext>(0);
  DurationLiteralContext? durationLiteral() => getRuleContext<DurationLiteralContext>(0);
  BinaryLiteralContext? binaryLiteral() => getRuleContext<BinaryLiteralContext>(0);
  GenericMapLiteralContext? genericMapLiteral() => getRuleContext<GenericMapLiteralContext>(0);
  GenericLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class GenericMapLiteralContext extends ParserRuleContext {
  TerminalNode? LBRACK() => getToken(GremlinParser.TOKEN_LBRACK, 0);
  TerminalNode? COLON() => getToken(GremlinParser.TOKEN_COLON, 0);
  TerminalNode? RBRACK() => getToken(GremlinParser.TOKEN_RBRACK, 0);
  List<MapEntryContext> mapEntrys() => getRuleContexts<MapEntryContext>();
  MapEntryContext? mapEntry(int i) => getRuleContext<MapEntryContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericMapLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_genericMapLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitGenericMapLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class MapKeyContext extends ParserRuleContext {
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalTLongContext? traversalTLong() => getRuleContext<TraversalTLongContext>(0);
  TraversalDirectionContext? traversalDirection() => getRuleContext<TraversalDirectionContext>(0);
  TraversalDirectionLongContext? traversalDirectionLong() => getRuleContext<TraversalDirectionLongContext>(0);
  GenericSetLiteralContext? genericSetLiteral() => getRuleContext<GenericSetLiteralContext>(0);
  GenericCollectionLiteralContext? genericCollectionLiteral() => getRuleContext<GenericCollectionLiteralContext>(0);
  GenericMapLiteralContext? genericMapLiteral() => getRuleContext<GenericMapLiteralContext>(0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  NumericLiteralContext? numericLiteral() => getRuleContext<NumericLiteralContext>(0);
  KeywordContext? keyword() => getRuleContext<KeywordContext>(0);
  NakedKeyContext? nakedKey() => getRuleContext<NakedKeyContext>(0);
  MapKeyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_mapKey;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitMapKey(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class MapEntryContext extends ParserRuleContext {
  MapKeyContext? mapKey() => getRuleContext<MapKeyContext>(0);
  TerminalNode? COLON() => getToken(GremlinParser.TOKEN_COLON, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  MapEntryContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_mapEntry;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitMapEntry(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringLiteralContext extends ParserRuleContext {
  TerminalNode? EmptyStringLiteral() => getToken(GremlinParser.TOKEN_EmptyStringLiteral, 0);
  TerminalNode? NonEmptyStringLiteral() => getToken(GremlinParser.TOKEN_NonEmptyStringLiteral, 0);
  TerminalNode? EmptyStringSuffixLiteral() => getToken(GremlinParser.TOKEN_EmptyStringSuffixLiteral, 0);
  TerminalNode? StringSuffixLiteral() => getToken(GremlinParser.TOKEN_StringSuffixLiteral, 0);
  StringLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class StringNullableLiteralContext extends ParserRuleContext {
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? K_NULL() => getToken(GremlinParser.TOKEN_K_NULL, 0);
  StringNullableLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_stringNullableLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitStringNullableLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class IntegerLiteralContext extends ParserRuleContext {
  TerminalNode? IntegerLiteral() => getToken(GremlinParser.TOKEN_IntegerLiteral, 0);
  IntegerLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_integerLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitIntegerLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class FloatLiteralContext extends ParserRuleContext {
  TerminalNode? FloatingPointLiteral() => getToken(GremlinParser.TOKEN_FloatingPointLiteral, 0);
  InfLiteralContext? infLiteral() => getRuleContext<InfLiteralContext>(0);
  NanLiteralContext? nanLiteral() => getRuleContext<NanLiteralContext>(0);
  FloatLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_floatLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitFloatLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NumericLiteralContext extends ParserRuleContext {
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  FloatLiteralContext? floatLiteral() => getRuleContext<FloatLiteralContext>(0);
  NumericLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_numericLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNumericLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class BooleanLiteralContext extends ParserRuleContext {
  TerminalNode? K_TRUE() => getToken(GremlinParser.TOKEN_K_TRUE, 0);
  TerminalNode? K_FALSE() => getToken(GremlinParser.TOKEN_K_FALSE, 0);
  BooleanLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_booleanLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitBooleanLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class DateLiteralContext extends ParserRuleContext {
  TerminalNode? K_DATETIME() => getToken(GremlinParser.TOKEN_K_DATETIME, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? K_DATETIMEC() => getToken(GremlinParser.TOKEN_K_DATETIMEC, 0);
  DateLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_dateLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitDateLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NullLiteralContext extends ParserRuleContext {
  TerminalNode? K_NULL() => getToken(GremlinParser.TOKEN_K_NULL, 0);
  NullLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nullLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNullLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NanLiteralContext extends ParserRuleContext {
  TerminalNode? K_NAN() => getToken(GremlinParser.TOKEN_K_NAN, 0);
  NanLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nanLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNanLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class InfLiteralContext extends ParserRuleContext {
  TerminalNode? K_INFINITY() => getToken(GremlinParser.TOKEN_K_INFINITY, 0);
  TerminalNode? SignedInfLiteral() => getToken(GremlinParser.TOKEN_SignedInfLiteral, 0);
  InfLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_infLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitInfLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class UuidLiteralContext extends ParserRuleContext {
  TerminalNode? K_UUID() => getToken(GremlinParser.TOKEN_K_UUID, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  UuidLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_uuidLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitUuidLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class CharacterLiteralContext extends ParserRuleContext {
  TerminalNode? CharacterLiteral() => getToken(GremlinParser.TOKEN_CharacterLiteral, 0);
  CharacterLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_characterLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitCharacterLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class DurationLiteralContext extends ParserRuleContext {
  TerminalNode? K_DURATIONC() => getToken(GremlinParser.TOKEN_K_DURATIONC, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<IntegerLiteralContext> integerLiterals() => getRuleContexts<IntegerLiteralContext>();
  IntegerLiteralContext? integerLiteral(int i) => getRuleContext<IntegerLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  BooleanLiteralContext? booleanLiteral() => getRuleContext<BooleanLiteralContext>(0);
  DurationLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_durationLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitDurationLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class BinaryLiteralContext extends ParserRuleContext {
  TerminalNode? K_BINARYC() => getToken(GremlinParser.TOKEN_K_BINARYC, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  BinaryLiteralContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_binaryLiteral;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitBinaryLiteral(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class NakedKeyContext extends ParserRuleContext {
  TerminalNode? Identifier() => getToken(GremlinParser.TOKEN_Identifier, 0);
  NakedKeyContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_nakedKey;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitNakedKey(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class ClassTypeContext extends ParserRuleContext {
  TerminalNode? Identifier() => getToken(GremlinParser.TOKEN_Identifier, 0);
  ClassTypeContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_classType;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitClassType(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class VariableContext extends ParserRuleContext {
  TerminalNode? Identifier() => getToken(GremlinParser.TOKEN_Identifier, 0);
  VariableContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_variable;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitVariable(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class KeywordContext extends ParserRuleContext {
  TerminalNode? TRAVERSAL_ROOT() => getToken(GremlinParser.TOKEN_TRAVERSAL_ROOT, 0);
  TerminalNode? K_ADDALL() => getToken(GremlinParser.TOKEN_K_ADDALL, 0);
  TerminalNode? K_ADDE() => getToken(GremlinParser.TOKEN_K_ADDE, 0);
  TerminalNode? K_ADDV() => getToken(GremlinParser.TOKEN_K_ADDV, 0);
  TerminalNode? K_AGGREGATE() => getToken(GremlinParser.TOKEN_K_AGGREGATE, 0);
  TerminalNode? K_ALL() => getToken(GremlinParser.TOKEN_K_ALL, 0);
  TerminalNode? K_AND() => getToken(GremlinParser.TOKEN_K_AND, 0);
  TerminalNode? K_ANY() => getToken(GremlinParser.TOKEN_K_ANY, 0);
  TerminalNode? K_AS() => getToken(GremlinParser.TOKEN_K_AS, 0);
  TerminalNode? K_ASBOOL() => getToken(GremlinParser.TOKEN_K_ASBOOL, 0);
  TerminalNode? K_ASC() => getToken(GremlinParser.TOKEN_K_ASC, 0);
  TerminalNode? K_ASDATE() => getToken(GremlinParser.TOKEN_K_ASDATE, 0);
  TerminalNode? K_ASNUMBER() => getToken(GremlinParser.TOKEN_K_ASNUMBER, 0);
  TerminalNode? K_ASSTRING() => getToken(GremlinParser.TOKEN_K_ASSTRING, 0);
  TerminalNode? K_ASSIGN() => getToken(GremlinParser.TOKEN_K_ASSIGN, 0);
  TerminalNode? K_BARRIER() => getToken(GremlinParser.TOKEN_K_BARRIER, 0);
  TerminalNode? K_BARRIERU() => getToken(GremlinParser.TOKEN_K_BARRIERU, 0);
  TerminalNode? K_BEGIN() => getToken(GremlinParser.TOKEN_K_BEGIN, 0);
  TerminalNode? K_BETWEEN() => getToken(GremlinParser.TOKEN_K_BETWEEN, 0);
  TerminalNode? K_BIGDECIMAL() => getToken(GremlinParser.TOKEN_K_BIGDECIMAL, 0);
  TerminalNode? K_BIGDECIMALU() => getToken(GremlinParser.TOKEN_K_BIGDECIMALU, 0);
  TerminalNode? K_BIGINT() => getToken(GremlinParser.TOKEN_K_BIGINT, 0);
  TerminalNode? K_BIGINTU() => getToken(GremlinParser.TOKEN_K_BIGINTU, 0);
  TerminalNode? K_BINARY() => getToken(GremlinParser.TOKEN_K_BINARY, 0);
  TerminalNode? K_BINARYC() => getToken(GremlinParser.TOKEN_K_BINARYC, 0);
  TerminalNode? K_BINARYU() => getToken(GremlinParser.TOKEN_K_BINARYU, 0);
  TerminalNode? K_BOTH() => getToken(GremlinParser.TOKEN_K_BOTH, 0);
  TerminalNode? K_BOTHU() => getToken(GremlinParser.TOKEN_K_BOTHU, 0);
  TerminalNode? K_BOTHE() => getToken(GremlinParser.TOKEN_K_BOTHE, 0);
  TerminalNode? K_BOTHV() => getToken(GremlinParser.TOKEN_K_BOTHV, 0);
  TerminalNode? K_BRANCH() => getToken(GremlinParser.TOKEN_K_BRANCH, 0);
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? K_BYTE() => getToken(GremlinParser.TOKEN_K_BYTE, 0);
  TerminalNode? K_BYTEU() => getToken(GremlinParser.TOKEN_K_BYTEU, 0);
  TerminalNode? K_BOOLEAN() => getToken(GremlinParser.TOKEN_K_BOOLEAN, 0);
  TerminalNode? K_BOOLEANU() => getToken(GremlinParser.TOKEN_K_BOOLEANU, 0);
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? K_CAP() => getToken(GremlinParser.TOKEN_K_CAP, 0);
  TerminalNode? K_CARDINALITY() => getToken(GremlinParser.TOKEN_K_CARDINALITY, 0);
  TerminalNode? K_CHAR() => getToken(GremlinParser.TOKEN_K_CHAR, 0);
  TerminalNode? K_CHARU() => getToken(GremlinParser.TOKEN_K_CHARU, 0);
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? K_COALESCE() => getToken(GremlinParser.TOKEN_K_COALESCE, 0);
  TerminalNode? K_COIN() => getToken(GremlinParser.TOKEN_K_COIN, 0);
  TerminalNode? K_COLUMN() => getToken(GremlinParser.TOKEN_K_COLUMN, 0);
  TerminalNode? K_COMBINE() => getToken(GremlinParser.TOKEN_K_COMBINE, 0);
  TerminalNode? K_CONCAT() => getToken(GremlinParser.TOKEN_K_CONCAT, 0);
  TerminalNode? K_COMMIT() => getToken(GremlinParser.TOKEN_K_COMMIT, 0);
  TerminalNode? K_COMPONENT() => getToken(GremlinParser.TOKEN_K_COMPONENT, 0);
  TerminalNode? K_CONJOIN() => getToken(GremlinParser.TOKEN_K_CONJOIN, 0);
  TerminalNode? K_CONNECTEDCOMPONENT() => getToken(GremlinParser.TOKEN_K_CONNECTEDCOMPONENT, 0);
  TerminalNode? K_CONNECTEDCOMPONENTU() => getToken(GremlinParser.TOKEN_K_CONNECTEDCOMPONENTU, 0);
  TerminalNode? K_CONSTANT() => getToken(GremlinParser.TOKEN_K_CONSTANT, 0);
  TerminalNode? K_CONTAINING() => getToken(GremlinParser.TOKEN_K_CONTAINING, 0);
  TerminalNode? K_COUNT() => getToken(GremlinParser.TOKEN_K_COUNT, 0);
  TerminalNode? K_CYCLICPATH() => getToken(GremlinParser.TOKEN_K_CYCLICPATH, 0);
  TerminalNode? K_DAY() => getToken(GremlinParser.TOKEN_K_DAY, 0);
  TerminalNode? K_DATEADD() => getToken(GremlinParser.TOKEN_K_DATEADD, 0);
  TerminalNode? K_DATEDIFF() => getToken(GremlinParser.TOKEN_K_DATEDIFF, 0);
  TerminalNode? K_DATETIME() => getToken(GremlinParser.TOKEN_K_DATETIME, 0);
  TerminalNode? K_DATETIMEC() => getToken(GremlinParser.TOKEN_K_DATETIMEC, 0);
  TerminalNode? K_DATETIMEU() => getToken(GremlinParser.TOKEN_K_DATETIMEU, 0);
  TerminalNode? K_DECR() => getToken(GremlinParser.TOKEN_K_DECR, 0);
  TerminalNode? K_DEDUP() => getToken(GremlinParser.TOKEN_K_DEDUP, 0);
  TerminalNode? K_DESC() => getToken(GremlinParser.TOKEN_K_DESC, 0);
  TerminalNode? K_DIFFERENCE() => getToken(GremlinParser.TOKEN_K_DIFFERENCE, 0);
  TerminalNode? K_DIRECTION() => getToken(GremlinParser.TOKEN_K_DIRECTION, 0);
  TerminalNode? K_DISCARD() => getToken(GremlinParser.TOKEN_K_DISCARD, 0);
  TerminalNode? K_DISJUNCT() => getToken(GremlinParser.TOKEN_K_DISJUNCT, 0);
  TerminalNode? K_DISTANCE() => getToken(GremlinParser.TOKEN_K_DISTANCE, 0);
  TerminalNode? K_DIV() => getToken(GremlinParser.TOKEN_K_DIV, 0);
  TerminalNode? K_DOUBLE() => getToken(GremlinParser.TOKEN_K_DOUBLE, 0);
  TerminalNode? K_DOUBLEU() => getToken(GremlinParser.TOKEN_K_DOUBLEU, 0);
  TerminalNode? K_DROP() => getToken(GremlinParser.TOKEN_K_DROP, 0);
  TerminalNode? K_DT() => getToken(GremlinParser.TOKEN_K_DT, 0);
  TerminalNode? K_DURATION() => getToken(GremlinParser.TOKEN_K_DURATION, 0);
  TerminalNode? K_DURATIONC() => getToken(GremlinParser.TOKEN_K_DURATIONC, 0);
  TerminalNode? K_DURATIONU() => getToken(GremlinParser.TOKEN_K_DURATIONU, 0);
  TerminalNode? K_E() => getToken(GremlinParser.TOKEN_K_E, 0);
  TerminalNode? K_EDGE() => getToken(GremlinParser.TOKEN_K_EDGE, 0);
  TerminalNode? K_EDGEU() => getToken(GremlinParser.TOKEN_K_EDGEU, 0);
  TerminalNode? K_EDGES() => getToken(GremlinParser.TOKEN_K_EDGES, 0);
  TerminalNode? K_ELEMENTMAP() => getToken(GremlinParser.TOKEN_K_ELEMENTMAP, 0);
  TerminalNode? K_ELEMENT() => getToken(GremlinParser.TOKEN_K_ELEMENT, 0);
  TerminalNode? K_EMIT() => getToken(GremlinParser.TOKEN_K_EMIT, 0);
  TerminalNode? K_ENDINGWITH() => getToken(GremlinParser.TOKEN_K_ENDINGWITH, 0);
  TerminalNode? K_EQ() => getToken(GremlinParser.TOKEN_K_EQ, 0);
  TerminalNode? K_EXPLAIN() => getToken(GremlinParser.TOKEN_K_EXPLAIN, 0);
  TerminalNode? K_FAIL() => getToken(GremlinParser.TOKEN_K_FAIL, 0);
  TerminalNode? K_FALSE() => getToken(GremlinParser.TOKEN_K_FALSE, 0);
  TerminalNode? K_FILTER() => getToken(GremlinParser.TOKEN_K_FILTER, 0);
  TerminalNode? K_FIRST() => getToken(GremlinParser.TOKEN_K_FIRST, 0);
  TerminalNode? K_FLATMAP() => getToken(GremlinParser.TOKEN_K_FLATMAP, 0);
  TerminalNode? K_FLOAT() => getToken(GremlinParser.TOKEN_K_FLOAT, 0);
  TerminalNode? K_FLOATU() => getToken(GremlinParser.TOKEN_K_FLOATU, 0);
  TerminalNode? K_FOLD() => getToken(GremlinParser.TOKEN_K_FOLD, 0);
  TerminalNode? K_FORMAT() => getToken(GremlinParser.TOKEN_K_FORMAT, 0);
  TerminalNode? K_FROM() => getToken(GremlinParser.TOKEN_K_FROM, 0);
  TerminalNode? K_GLOBAL() => getToken(GremlinParser.TOKEN_K_GLOBAL, 0);
  TerminalNode? K_GT() => getToken(GremlinParser.TOKEN_K_GT, 0);
  TerminalNode? K_GTE() => getToken(GremlinParser.TOKEN_K_GTE, 0);
  TerminalNode? K_GRAPH() => getToken(GremlinParser.TOKEN_K_GRAPH, 0);
  TerminalNode? K_GRAPHU() => getToken(GremlinParser.TOKEN_K_GRAPHU, 0);
  TerminalNode? K_GRAPHML() => getToken(GremlinParser.TOKEN_K_GRAPHML, 0);
  TerminalNode? K_GRAPHSON() => getToken(GremlinParser.TOKEN_K_GRAPHSON, 0);
  TerminalNode? K_GROUP() => getToken(GremlinParser.TOKEN_K_GROUP, 0);
  TerminalNode? K_GROUPCOUNT() => getToken(GremlinParser.TOKEN_K_GROUPCOUNT, 0);
  TerminalNode? K_GRYO() => getToken(GremlinParser.TOKEN_K_GRYO, 0);
  TerminalNode? K_GTYPE() => getToken(GremlinParser.TOKEN_K_GTYPE, 0);
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? K_HASID() => getToken(GremlinParser.TOKEN_K_HASID, 0);
  TerminalNode? K_HASKEY() => getToken(GremlinParser.TOKEN_K_HASKEY, 0);
  TerminalNode? K_HASLABEL() => getToken(GremlinParser.TOKEN_K_HASLABEL, 0);
  TerminalNode? K_HASNEXT() => getToken(GremlinParser.TOKEN_K_HASNEXT, 0);
  TerminalNode? K_HASNOT() => getToken(GremlinParser.TOKEN_K_HASNOT, 0);
  TerminalNode? K_HASVALUE() => getToken(GremlinParser.TOKEN_K_HASVALUE, 0);
  TerminalNode? K_HOUR() => getToken(GremlinParser.TOKEN_K_HOUR, 0);
  TerminalNode? K_ID() => getToken(GremlinParser.TOKEN_K_ID, 0);
  TerminalNode? K_IDENTITY() => getToken(GremlinParser.TOKEN_K_IDENTITY, 0);
  TerminalNode? K_IDS() => getToken(GremlinParser.TOKEN_K_IDS, 0);
  TerminalNode? K_IN() => getToken(GremlinParser.TOKEN_K_IN, 0);
  TerminalNode? K_INU() => getToken(GremlinParser.TOKEN_K_INU, 0);
  TerminalNode? K_INCLUDEEDGES() => getToken(GremlinParser.TOKEN_K_INCLUDEEDGES, 0);
  TerminalNode? K_INCR() => getToken(GremlinParser.TOKEN_K_INCR, 0);
  TerminalNode? K_INDEXER() => getToken(GremlinParser.TOKEN_K_INDEXER, 0);
  TerminalNode? K_INE() => getToken(GremlinParser.TOKEN_K_INE, 0);
  TerminalNode? K_INDEX() => getToken(GremlinParser.TOKEN_K_INDEX, 0);
  TerminalNode? K_INFINITY() => getToken(GremlinParser.TOKEN_K_INFINITY, 0);
  TerminalNode? K_INJECT() => getToken(GremlinParser.TOKEN_K_INJECT, 0);
  TerminalNode? K_INSIDE() => getToken(GremlinParser.TOKEN_K_INSIDE, 0);
  TerminalNode? K_INT() => getToken(GremlinParser.TOKEN_K_INT, 0);
  TerminalNode? K_INTU() => getToken(GremlinParser.TOKEN_K_INTU, 0);
  TerminalNode? K_INTERSECT() => getToken(GremlinParser.TOKEN_K_INTERSECT, 0);
  TerminalNode? K_INV() => getToken(GremlinParser.TOKEN_K_INV, 0);
  TerminalNode? K_IO() => getToken(GremlinParser.TOKEN_K_IO, 0);
  TerminalNode? K_IOU() => getToken(GremlinParser.TOKEN_K_IOU, 0);
  TerminalNode? K_IS() => getToken(GremlinParser.TOKEN_K_IS, 0);
  TerminalNode? K_ITERATE() => getToken(GremlinParser.TOKEN_K_ITERATE, 0);
  TerminalNode? K_KEY() => getToken(GremlinParser.TOKEN_K_KEY, 0);
  TerminalNode? K_KEYS() => getToken(GremlinParser.TOKEN_K_KEYS, 0);
  TerminalNode? K_LABELS() => getToken(GremlinParser.TOKEN_K_LABELS, 0);
  TerminalNode? K_LABEL() => getToken(GremlinParser.TOKEN_K_LABEL, 0);
  TerminalNode? K_LAST() => getToken(GremlinParser.TOKEN_K_LAST, 0);
  TerminalNode? K_LENGTH() => getToken(GremlinParser.TOKEN_K_LENGTH, 0);
  TerminalNode? K_LIMIT() => getToken(GremlinParser.TOKEN_K_LIMIT, 0);
  TerminalNode? K_LIST() => getToken(GremlinParser.TOKEN_K_LIST, 0);
  TerminalNode? K_LISTU() => getToken(GremlinParser.TOKEN_K_LISTU, 0);
  TerminalNode? K_LOCAL() => getToken(GremlinParser.TOKEN_K_LOCAL, 0);
  TerminalNode? K_LONG() => getToken(GremlinParser.TOKEN_K_LONG, 0);
  TerminalNode? K_LONGU() => getToken(GremlinParser.TOKEN_K_LONGU, 0);
  TerminalNode? K_LOOPS() => getToken(GremlinParser.TOKEN_K_LOOPS, 0);
  TerminalNode? K_LT() => getToken(GremlinParser.TOKEN_K_LT, 0);
  TerminalNode? K_LTE() => getToken(GremlinParser.TOKEN_K_LTE, 0);
  TerminalNode? K_LTRIM() => getToken(GremlinParser.TOKEN_K_LTRIM, 0);
  TerminalNode? K_MAP() => getToken(GremlinParser.TOKEN_K_MAP, 0);
  TerminalNode? K_MAPU() => getToken(GremlinParser.TOKEN_K_MAPU, 0);
  TerminalNode? K_MATCH() => getToken(GremlinParser.TOKEN_K_MATCH, 0);
  TerminalNode? K_MATH() => getToken(GremlinParser.TOKEN_K_MATH, 0);
  TerminalNode? K_MAX() => getToken(GremlinParser.TOKEN_K_MAX, 0);
  TerminalNode? K_MAXDISTANCE() => getToken(GremlinParser.TOKEN_K_MAXDISTANCE, 0);
  TerminalNode? K_MEAN() => getToken(GremlinParser.TOKEN_K_MEAN, 0);
  TerminalNode? K_MERGE() => getToken(GremlinParser.TOKEN_K_MERGE, 0);
  TerminalNode? K_MERGEU() => getToken(GremlinParser.TOKEN_K_MERGEU, 0);
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? K_MIN() => getToken(GremlinParser.TOKEN_K_MIN, 0);
  TerminalNode? K_MINUTE() => getToken(GremlinParser.TOKEN_K_MINUTE, 0);
  TerminalNode? K_MINUS() => getToken(GremlinParser.TOKEN_K_MINUS, 0);
  TerminalNode? K_MIXED() => getToken(GremlinParser.TOKEN_K_MIXED, 0);
  TerminalNode? K_MULT() => getToken(GremlinParser.TOKEN_K_MULT, 0);
  TerminalNode? K_N() => getToken(GremlinParser.TOKEN_K_N, 0);
  TerminalNode? K_NAN() => getToken(GremlinParser.TOKEN_K_NAN, 0);
  TerminalNode? K_NEGATE() => getToken(GremlinParser.TOKEN_K_NEGATE, 0);
  TerminalNode? K_NEW() => getToken(GremlinParser.TOKEN_K_NEW, 0);
  TerminalNode? K_NONE() => getToken(GremlinParser.TOKEN_K_NONE, 0);
  TerminalNode? K_NOTCONTAINING() => getToken(GremlinParser.TOKEN_K_NOTCONTAINING, 0);
  TerminalNode? K_NOTENDINGWITH() => getToken(GremlinParser.TOKEN_K_NOTENDINGWITH, 0);
  TerminalNode? K_NOTREGEX() => getToken(GremlinParser.TOKEN_K_NOTREGEX, 0);
  TerminalNode? K_NOTSTARTINGWITH() => getToken(GremlinParser.TOKEN_K_NOTSTARTINGWITH, 0);
  TerminalNode? K_NOT() => getToken(GremlinParser.TOKEN_K_NOT, 0);
  TerminalNode? K_NEQ() => getToken(GremlinParser.TOKEN_K_NEQ, 0);
  TerminalNode? K_NEXT() => getToken(GremlinParser.TOKEN_K_NEXT, 0);
  TerminalNode? K_NULL() => getToken(GremlinParser.TOKEN_K_NULL, 0);
  TerminalNode? K_NULLU() => getToken(GremlinParser.TOKEN_K_NULLU, 0);
  TerminalNode? K_NUMBER() => getToken(GremlinParser.TOKEN_K_NUMBER, 0);
  TerminalNode? K_NUMBERU() => getToken(GremlinParser.TOKEN_K_NUMBERU, 0);
  TerminalNode? K_NORMSACK() => getToken(GremlinParser.TOKEN_K_NORMSACK, 0);
  TerminalNode? K_ONCREATE() => getToken(GremlinParser.TOKEN_K_ONCREATE, 0);
  TerminalNode? K_ONMATCH() => getToken(GremlinParser.TOKEN_K_ONMATCH, 0);
  TerminalNode? K_OPERATOR() => getToken(GremlinParser.TOKEN_K_OPERATOR, 0);
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? K_OPTIONAL() => getToken(GremlinParser.TOKEN_K_OPTIONAL, 0);
  TerminalNode? K_ORDER() => getToken(GremlinParser.TOKEN_K_ORDER, 0);
  TerminalNode? K_ORDERU() => getToken(GremlinParser.TOKEN_K_ORDERU, 0);
  TerminalNode? K_OR() => getToken(GremlinParser.TOKEN_K_OR, 0);
  TerminalNode? K_OTHERV() => getToken(GremlinParser.TOKEN_K_OTHERV, 0);
  TerminalNode? K_OUT() => getToken(GremlinParser.TOKEN_K_OUT, 0);
  TerminalNode? K_OUTU() => getToken(GremlinParser.TOKEN_K_OUTU, 0);
  TerminalNode? K_OUTE() => getToken(GremlinParser.TOKEN_K_OUTE, 0);
  TerminalNode? K_OUTSIDE() => getToken(GremlinParser.TOKEN_K_OUTSIDE, 0);
  TerminalNode? K_OUTV() => getToken(GremlinParser.TOKEN_K_OUTV, 0);
  TerminalNode? K_P() => getToken(GremlinParser.TOKEN_K_P, 0);
  TerminalNode? K_PAGERANK() => getToken(GremlinParser.TOKEN_K_PAGERANK, 0);
  TerminalNode? K_PAGERANKU() => getToken(GremlinParser.TOKEN_K_PAGERANKU, 0);
  TerminalNode? K_PATH() => getToken(GremlinParser.TOKEN_K_PATH, 0);
  TerminalNode? K_PATHU() => getToken(GremlinParser.TOKEN_K_PATHU, 0);
  TerminalNode? K_PEERPRESSURE() => getToken(GremlinParser.TOKEN_K_PEERPRESSURE, 0);
  TerminalNode? K_PEERPRESSUREU() => getToken(GremlinParser.TOKEN_K_PEERPRESSUREU, 0);
  TerminalNode? K_PICK() => getToken(GremlinParser.TOKEN_K_PICK, 0);
  TerminalNode? K_POP() => getToken(GremlinParser.TOKEN_K_POP, 0);
  TerminalNode? K_PROFILE() => getToken(GremlinParser.TOKEN_K_PROFILE, 0);
  TerminalNode? K_PROJECT() => getToken(GremlinParser.TOKEN_K_PROJECT, 0);
  TerminalNode? K_PROPERTIES() => getToken(GremlinParser.TOKEN_K_PROPERTIES, 0);
  TerminalNode? K_PROPERTYMAP() => getToken(GremlinParser.TOKEN_K_PROPERTYMAP, 0);
  TerminalNode? K_PROPERTYNAME() => getToken(GremlinParser.TOKEN_K_PROPERTYNAME, 0);
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? K_PROPERTYU() => getToken(GremlinParser.TOKEN_K_PROPERTYU, 0);
  TerminalNode? K_PRODUCT() => getToken(GremlinParser.TOKEN_K_PRODUCT, 0);
  TerminalNode? K_RANGE() => getToken(GremlinParser.TOKEN_K_RANGE, 0);
  TerminalNode? K_READ() => getToken(GremlinParser.TOKEN_K_READ, 0);
  TerminalNode? K_READER() => getToken(GremlinParser.TOKEN_K_READER, 0);
  TerminalNode? K_REGEX() => getToken(GremlinParser.TOKEN_K_REGEX, 0);
  TerminalNode? K_REPLACE() => getToken(GremlinParser.TOKEN_K_REPLACE, 0);
  TerminalNode? K_REPEAT() => getToken(GremlinParser.TOKEN_K_REPEAT, 0);
  TerminalNode? K_REVERSE() => getToken(GremlinParser.TOKEN_K_REVERSE, 0);
  TerminalNode? K_ROLLBACK() => getToken(GremlinParser.TOKEN_K_ROLLBACK, 0);
  TerminalNode? K_RTRIM() => getToken(GremlinParser.TOKEN_K_RTRIM, 0);
  TerminalNode? K_SACK() => getToken(GremlinParser.TOKEN_K_SACK, 0);
  TerminalNode? K_SAMPLE() => getToken(GremlinParser.TOKEN_K_SAMPLE, 0);
  TerminalNode? K_SCOPE() => getToken(GremlinParser.TOKEN_K_SCOPE, 0);
  TerminalNode? K_SECOND() => getToken(GremlinParser.TOKEN_K_SECOND, 0);
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? K_SET() => getToken(GremlinParser.TOKEN_K_SET, 0);
  TerminalNode? K_SETU() => getToken(GremlinParser.TOKEN_K_SETU, 0);
  TerminalNode? K_SHORT() => getToken(GremlinParser.TOKEN_K_SHORT, 0);
  TerminalNode? K_SHORTU() => getToken(GremlinParser.TOKEN_K_SHORTU, 0);
  TerminalNode? K_SHORTESTPATH() => getToken(GremlinParser.TOKEN_K_SHORTESTPATH, 0);
  TerminalNode? K_SHORTESTPATHU() => getToken(GremlinParser.TOKEN_K_SHORTESTPATHU, 0);
  TerminalNode? K_SHUFFLE() => getToken(GremlinParser.TOKEN_K_SHUFFLE, 0);
  TerminalNode? K_SIDEEFFECT() => getToken(GremlinParser.TOKEN_K_SIDEEFFECT, 0);
  TerminalNode? K_SIMPLEPATH() => getToken(GremlinParser.TOKEN_K_SIMPLEPATH, 0);
  TerminalNode? K_SINGLE() => getToken(GremlinParser.TOKEN_K_SINGLE, 0);
  TerminalNode? K_SKIP() => getToken(GremlinParser.TOKEN_K_SKIP, 0);
  TerminalNode? K_SPLIT() => getToken(GremlinParser.TOKEN_K_SPLIT, 0);
  TerminalNode? K_STARTINGWITH() => getToken(GremlinParser.TOKEN_K_STARTINGWITH, 0);
  TerminalNode? K_STRING() => getToken(GremlinParser.TOKEN_K_STRING, 0);
  TerminalNode? K_STRINGU() => getToken(GremlinParser.TOKEN_K_STRINGU, 0);
  TerminalNode? K_SUBGRAPH() => getToken(GremlinParser.TOKEN_K_SUBGRAPH, 0);
  TerminalNode? K_SUBSTRING() => getToken(GremlinParser.TOKEN_K_SUBSTRING, 0);
  TerminalNode? K_SUM() => getToken(GremlinParser.TOKEN_K_SUM, 0);
  TerminalNode? K_SUMLONG() => getToken(GremlinParser.TOKEN_K_SUMLONG, 0);
  TerminalNode? K_T() => getToken(GremlinParser.TOKEN_K_T, 0);
  TerminalNode? K_TAIL() => getToken(GremlinParser.TOKEN_K_TAIL, 0);
  TerminalNode? K_TARGET() => getToken(GremlinParser.TOKEN_K_TARGET, 0);
  TerminalNode? K_TEXTP() => getToken(GremlinParser.TOKEN_K_TEXTP, 0);
  TerminalNode? K_TIMELIMIT() => getToken(GremlinParser.TOKEN_K_TIMELIMIT, 0);
  TerminalNode? K_TIMES() => getToken(GremlinParser.TOKEN_K_TIMES, 0);
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? K_TOBULKSET() => getToken(GremlinParser.TOKEN_K_TOBULKSET, 0);
  TerminalNode? K_TOKENS() => getToken(GremlinParser.TOKEN_K_TOKENS, 0);
  TerminalNode? K_TOLIST() => getToken(GremlinParser.TOKEN_K_TOLIST, 0);
  TerminalNode? K_TOLOWER() => getToken(GremlinParser.TOKEN_K_TOLOWER, 0);
  TerminalNode? K_TOSET() => getToken(GremlinParser.TOKEN_K_TOSET, 0);
  TerminalNode? K_TOSTRING() => getToken(GremlinParser.TOKEN_K_TOSTRING, 0);
  TerminalNode? K_TOUPPER() => getToken(GremlinParser.TOKEN_K_TOUPPER, 0);
  TerminalNode? K_TOE() => getToken(GremlinParser.TOKEN_K_TOE, 0);
  TerminalNode? K_TOV() => getToken(GremlinParser.TOKEN_K_TOV, 0);
  TerminalNode? K_TREE() => getToken(GremlinParser.TOKEN_K_TREE, 0);
  TerminalNode? K_TREEU() => getToken(GremlinParser.TOKEN_K_TREEU, 0);
  TerminalNode? K_TRIM() => getToken(GremlinParser.TOKEN_K_TRIM, 0);
  TerminalNode? K_TRUE() => getToken(GremlinParser.TOKEN_K_TRUE, 0);
  TerminalNode? K_TRYNEXT() => getToken(GremlinParser.TOKEN_K_TRYNEXT, 0);
  TerminalNode? K_TYPEOF() => getToken(GremlinParser.TOKEN_K_TYPEOF, 0);
  TerminalNode? K_TX() => getToken(GremlinParser.TOKEN_K_TX, 0);
  TerminalNode? K_UNFOLD() => getToken(GremlinParser.TOKEN_K_UNFOLD, 0);
  TerminalNode? K_UNION() => getToken(GremlinParser.TOKEN_K_UNION, 0);
  TerminalNode? K_UNPRODUCTIVE() => getToken(GremlinParser.TOKEN_K_UNPRODUCTIVE, 0);
  TerminalNode? K_UNTIL() => getToken(GremlinParser.TOKEN_K_UNTIL, 0);
  TerminalNode? K_UUID() => getToken(GremlinParser.TOKEN_K_UUID, 0);
  TerminalNode? K_UUIDL() => getToken(GremlinParser.TOKEN_K_UUIDL, 0);
  TerminalNode? K_V() => getToken(GremlinParser.TOKEN_K_V, 0);
  TerminalNode? K_VALUEMAP() => getToken(GremlinParser.TOKEN_K_VALUEMAP, 0);
  TerminalNode? K_VALUES() => getToken(GremlinParser.TOKEN_K_VALUES, 0);
  TerminalNode? K_VALUE() => getToken(GremlinParser.TOKEN_K_VALUE, 0);
  TerminalNode? K_VERTEX() => getToken(GremlinParser.TOKEN_K_VERTEX, 0);
  TerminalNode? K_VERTEXU() => getToken(GremlinParser.TOKEN_K_VERTEXU, 0);
  TerminalNode? K_VPROPERTY() => getToken(GremlinParser.TOKEN_K_VPROPERTY, 0);
  TerminalNode? K_VPROPERTYU() => getToken(GremlinParser.TOKEN_K_VPROPERTYU, 0);
  TerminalNode? K_WHERE() => getToken(GremlinParser.TOKEN_K_WHERE, 0);
  TerminalNode? K_WITH() => getToken(GremlinParser.TOKEN_K_WITH, 0);
  TerminalNode? K_WITHBULK() => getToken(GremlinParser.TOKEN_K_WITHBULK, 0);
  TerminalNode? K_WITHIN() => getToken(GremlinParser.TOKEN_K_WITHIN, 0);
  TerminalNode? K_WITHOPTOPTIONS() => getToken(GremlinParser.TOKEN_K_WITHOPTOPTIONS, 0);
  TerminalNode? K_WITHOUT() => getToken(GremlinParser.TOKEN_K_WITHOUT, 0);
  TerminalNode? K_WITHOUTSTRATEGIES() => getToken(GremlinParser.TOKEN_K_WITHOUTSTRATEGIES, 0);
  TerminalNode? K_WITHPATH() => getToken(GremlinParser.TOKEN_K_WITHPATH, 0);
  TerminalNode? K_WITHSACK() => getToken(GremlinParser.TOKEN_K_WITHSACK, 0);
  TerminalNode? K_WITHSIDEEFFECT() => getToken(GremlinParser.TOKEN_K_WITHSIDEEFFECT, 0);
  TerminalNode? K_WITHSTRATEGIES() => getToken(GremlinParser.TOKEN_K_WITHSTRATEGIES, 0);
  TerminalNode? K_WRITE() => getToken(GremlinParser.TOKEN_K_WRITE, 0);
  TerminalNode? K_WRITER() => getToken(GremlinParser.TOKEN_K_WRITER, 0);
  KeywordContext([ParserRuleContext? parent, int? invokingState]) : super(parent, invokingState);
  @override
  int get ruleIndex => RULE_keyword;
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitKeyword(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_mergeV_TraversalContext extends TraversalSourceSpawnMethod_mergeVContext {
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_mergeV_TraversalContext(TraversalSourceSpawnMethod_mergeVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_mergeV_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_mergeV_MapContext extends TraversalSourceSpawnMethod_mergeVContext {
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_mergeV_MapContext(TraversalSourceSpawnMethod_mergeVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_mergeV_Map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalSourceSpawnMethod_mergeE_TraversalContext extends TraversalSourceSpawnMethod_mergeEContext {
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_mergeE_TraversalContext(TraversalSourceSpawnMethod_mergeEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_mergeE_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_mergeE_MapContext extends TraversalSourceSpawnMethod_mergeEContext {
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_mergeE_MapContext(TraversalSourceSpawnMethod_mergeEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_mergeE_Map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalSourceSpawnMethod_call_emptyContext extends TraversalSourceSpawnMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_call_emptyContext(TraversalSourceSpawnMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_call_empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_call_string_traversalContext extends TraversalSourceSpawnMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_call_string_traversalContext(TraversalSourceSpawnMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_call_string_traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_call_string_mapContext extends TraversalSourceSpawnMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericMapArgumentContext? genericMapArgument() => getRuleContext<GenericMapArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_call_string_mapContext(TraversalSourceSpawnMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_call_string_map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_call_stringContext extends TraversalSourceSpawnMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_call_stringContext(TraversalSourceSpawnMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_call_string(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalSourceSpawnMethod_call_string_map_traversalContext extends TraversalSourceSpawnMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericMapArgumentContext? genericMapArgument() => getRuleContext<GenericMapArgumentContext>(0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalSourceSpawnMethod_call_string_map_traversalContext(TraversalSourceSpawnMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalSourceSpawnMethod_call_string_map_traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_addE_TraversalContext extends TraversalMethod_addEContext {
  TerminalNode? K_ADDE() => getToken(GremlinParser.TOKEN_K_ADDE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_addE_TraversalContext(TraversalMethod_addEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_addE_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_addE_StringContext extends TraversalMethod_addEContext {
  TerminalNode? K_ADDE() => getToken(GremlinParser.TOKEN_K_ADDE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_addE_StringContext(TraversalMethod_addEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_addE_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_addV_EmptyContext extends TraversalMethod_addVContext {
  TerminalNode? K_ADDV() => getToken(GremlinParser.TOKEN_K_ADDV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_addV_EmptyContext(TraversalMethod_addVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_addV_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_addV_StringContext extends TraversalMethod_addVContext {
  TerminalNode? K_ADDV() => getToken(GremlinParser.TOKEN_K_ADDV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringArgumentContext? stringArgument() => getRuleContext<StringArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_addV_StringContext(TraversalMethod_addVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_addV_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_addV_TraversalContext extends TraversalMethod_addVContext {
  TerminalNode? K_ADDV() => getToken(GremlinParser.TOKEN_K_ADDV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_addV_TraversalContext(TraversalMethod_addVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_addV_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_aggregate_StringContext extends TraversalMethod_aggregateContext {
  TerminalNode? K_AGGREGATE() => getToken(GremlinParser.TOKEN_K_AGGREGATE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_aggregate_StringContext(TraversalMethod_aggregateContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_aggregate_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_all_PContext extends TraversalMethod_allContext {
  TerminalNode? K_ALL() => getToken(GremlinParser.TOKEN_K_ALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_all_PContext(TraversalMethod_allContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_all_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_any_PContext extends TraversalMethod_anyContext {
  TerminalNode? K_ANY() => getToken(GremlinParser.TOKEN_K_ANY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_any_PContext(TraversalMethod_anyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_any_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_asNumber_EmptyContext extends TraversalMethod_asNumberContext {
  TerminalNode? K_ASNUMBER() => getToken(GremlinParser.TOKEN_K_ASNUMBER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asNumber_EmptyContext(TraversalMethod_asNumberContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asNumber_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_asNumber_traversalGTypeContext extends TraversalMethod_asNumberContext {
  TerminalNode? K_ASNUMBER() => getToken(GremlinParser.TOKEN_K_ASNUMBER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalGTypeContext? traversalGType() => getRuleContext<TraversalGTypeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asNumber_traversalGTypeContext(TraversalMethod_asNumberContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asNumber_traversalGType(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_asString_EmptyContext extends TraversalMethod_asStringContext {
  TerminalNode? K_ASSTRING() => getToken(GremlinParser.TOKEN_K_ASSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asString_EmptyContext(TraversalMethod_asStringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asString_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_asString_ScopeContext extends TraversalMethod_asStringContext {
  TerminalNode? K_ASSTRING() => getToken(GremlinParser.TOKEN_K_ASSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_asString_ScopeContext(TraversalMethod_asStringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_asString_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_barrier_ConsumerContext extends TraversalMethod_barrierContext {
  TerminalNode? K_BARRIER() => getToken(GremlinParser.TOKEN_K_BARRIER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalSackMethodContext? traversalSackMethod() => getRuleContext<TraversalSackMethodContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_barrier_ConsumerContext(TraversalMethod_barrierContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_barrier_Consumer(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_barrier_intContext extends TraversalMethod_barrierContext {
  TerminalNode? K_BARRIER() => getToken(GremlinParser.TOKEN_K_BARRIER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_barrier_intContext(TraversalMethod_barrierContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_barrier_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_barrier_EmptyContext extends TraversalMethod_barrierContext {
  TerminalNode? K_BARRIER() => getToken(GremlinParser.TOKEN_K_BARRIER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_barrier_EmptyContext(TraversalMethod_barrierContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_barrier_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_by_StringContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_StringContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_String_ComparatorContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalComparatorContext? traversalComparator() => getRuleContext<TraversalComparatorContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_String_ComparatorContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_String_Comparator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_FunctionContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalFunctionContext? traversalFunction() => getRuleContext<TraversalFunctionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_FunctionContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Function(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_TraversalContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_TraversalContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_EmptyContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_EmptyContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_ComparatorContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalComparatorContext? traversalComparator() => getRuleContext<TraversalComparatorContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_ComparatorContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Comparator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_OrderContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalOrderContext? traversalOrder() => getRuleContext<TraversalOrderContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_OrderContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Order(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_Function_ComparatorContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalFunctionContext? traversalFunction() => getRuleContext<TraversalFunctionContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalComparatorContext? traversalComparator() => getRuleContext<TraversalComparatorContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_Function_ComparatorContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Function_Comparator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_TContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_TContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_T(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_by_Traversal_ComparatorContext extends TraversalMethod_byContext {
  TerminalNode? K_BY() => getToken(GremlinParser.TOKEN_K_BY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalComparatorContext? traversalComparator() => getRuleContext<TraversalComparatorContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_by_Traversal_ComparatorContext(TraversalMethod_byContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_by_Traversal_Comparator(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_call_string_mapContext extends TraversalMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericMapArgumentContext? genericMapArgument() => getRuleContext<GenericMapArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_call_string_mapContext(TraversalMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_call_string_map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_call_string_map_traversalContext extends TraversalMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericMapArgumentContext? genericMapArgument() => getRuleContext<GenericMapArgumentContext>(0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_call_string_map_traversalContext(TraversalMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_call_string_map_traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_call_string_traversalContext extends TraversalMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_call_string_traversalContext(TraversalMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_call_string_traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_call_stringContext extends TraversalMethod_callContext {
  TerminalNode? K_CALL() => getToken(GremlinParser.TOKEN_K_CALL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_call_stringContext(TraversalMethod_callContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_call_string(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_choose_TraversalContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_TraversalContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_choose_FunctionContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalFunctionContext? traversalFunction() => getRuleContext<TraversalFunctionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_FunctionContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Function(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_choose_Traversal_TraversalContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<NestedTraversalContext> nestedTraversals() => getRuleContexts<NestedTraversalContext>();
  NestedTraversalContext? nestedTraversal(int i) => getRuleContext<NestedTraversalContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_Traversal_TraversalContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Traversal_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_choose_Predicate_TraversalContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_Predicate_TraversalContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Predicate_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_choose_Predicate_Traversal_TraversalContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  List<NestedTraversalContext> nestedTraversals() => getRuleContexts<NestedTraversalContext>();
  NestedTraversalContext? nestedTraversal(int i) => getRuleContext<NestedTraversalContext>(i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_Predicate_Traversal_TraversalContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Predicate_Traversal_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_choose_Traversal_Traversal_TraversalContext extends TraversalMethod_chooseContext {
  TerminalNode? K_CHOOSE() => getToken(GremlinParser.TOKEN_K_CHOOSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<NestedTraversalContext> nestedTraversals() => getRuleContexts<NestedTraversalContext>();
  NestedTraversalContext? nestedTraversal(int i) => getRuleContext<NestedTraversalContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_choose_Traversal_Traversal_TraversalContext(TraversalMethod_chooseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_choose_Traversal_Traversal_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_combine_ObjectContext extends TraversalMethod_combineContext {
  TerminalNode? K_COMBINE() => getToken(GremlinParser.TOKEN_K_COMBINE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_combine_ObjectContext(TraversalMethod_combineContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_combine_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_concat_Traversal_TraversalContext extends TraversalMethod_concatContext {
  TerminalNode? K_CONCAT() => getToken(GremlinParser.TOKEN_K_CONCAT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalListContext? nestedTraversalList() => getRuleContext<NestedTraversalListContext>(0);
  TraversalMethod_concat_Traversal_TraversalContext(TraversalMethod_concatContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_concat_Traversal_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_concat_StringContext extends TraversalMethod_concatContext {
  TerminalNode? K_CONCAT() => getToken(GremlinParser.TOKEN_K_CONCAT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_concat_StringContext(TraversalMethod_concatContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_concat_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_conjoin_StringContext extends TraversalMethod_conjoinContext {
  TerminalNode? K_CONJOIN() => getToken(GremlinParser.TOKEN_K_CONJOIN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_conjoin_StringContext(TraversalMethod_conjoinContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_conjoin_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_count_ScopeContext extends TraversalMethod_countContext {
  TerminalNode? K_COUNT() => getToken(GremlinParser.TOKEN_K_COUNT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_count_ScopeContext(TraversalMethod_countContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_count_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_count_EmptyContext extends TraversalMethod_countContext {
  TerminalNode? K_COUNT() => getToken(GremlinParser.TOKEN_K_COUNT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_count_EmptyContext(TraversalMethod_countContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_count_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_dateDiff_TraversalContext extends TraversalMethod_dateDiffContext {
  TerminalNode? K_DATEDIFF() => getToken(GremlinParser.TOKEN_K_DATEDIFF, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_dateDiff_TraversalContext(TraversalMethod_dateDiffContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_dateDiff_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_dateDiff_DateContext extends TraversalMethod_dateDiffContext {
  TerminalNode? K_DATEDIFF() => getToken(GremlinParser.TOKEN_K_DATEDIFF, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  DateLiteralContext? dateLiteral() => getRuleContext<DateLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_dateDiff_DateContext(TraversalMethod_dateDiffContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_dateDiff_Date(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_dedup_Scope_StringContext extends TraversalMethod_dedupContext {
  TerminalNode? K_DEDUP() => getToken(GremlinParser.TOKEN_K_DEDUP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_dedup_Scope_StringContext(TraversalMethod_dedupContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_dedup_Scope_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_dedup_StringContext extends TraversalMethod_dedupContext {
  TerminalNode? K_DEDUP() => getToken(GremlinParser.TOKEN_K_DEDUP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_dedup_StringContext(TraversalMethod_dedupContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_dedup_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_difference_ObjectContext extends TraversalMethod_differenceContext {
  TerminalNode? K_DIFFERENCE() => getToken(GremlinParser.TOKEN_K_DIFFERENCE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_difference_ObjectContext(TraversalMethod_differenceContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_difference_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_disjunct_ObjectContext extends TraversalMethod_disjunctContext {
  TerminalNode? K_DISJUNCT() => getToken(GremlinParser.TOKEN_K_DISJUNCT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_disjunct_ObjectContext(TraversalMethod_disjunctContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_disjunct_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_emit_EmptyContext extends TraversalMethod_emitContext {
  TerminalNode? K_EMIT() => getToken(GremlinParser.TOKEN_K_EMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_emit_EmptyContext(TraversalMethod_emitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_emit_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_emit_PredicateContext extends TraversalMethod_emitContext {
  TerminalNode? K_EMIT() => getToken(GremlinParser.TOKEN_K_EMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_emit_PredicateContext(TraversalMethod_emitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_emit_Predicate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_emit_TraversalContext extends TraversalMethod_emitContext {
  TerminalNode? K_EMIT() => getToken(GremlinParser.TOKEN_K_EMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_emit_TraversalContext(TraversalMethod_emitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_emit_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_fail_StringContext extends TraversalMethod_failContext {
  TerminalNode? K_FAIL() => getToken(GremlinParser.TOKEN_K_FAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_fail_StringContext(TraversalMethod_failContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_fail_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_fail_EmptyContext extends TraversalMethod_failContext {
  TerminalNode? K_FAIL() => getToken(GremlinParser.TOKEN_K_FAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_fail_EmptyContext(TraversalMethod_failContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_fail_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_filter_PredicateContext extends TraversalMethod_filterContext {
  TerminalNode? K_FILTER() => getToken(GremlinParser.TOKEN_K_FILTER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_filter_PredicateContext(TraversalMethod_filterContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_filter_Predicate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_filter_TraversalContext extends TraversalMethod_filterContext {
  TerminalNode? K_FILTER() => getToken(GremlinParser.TOKEN_K_FILTER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_filter_TraversalContext(TraversalMethod_filterContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_filter_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_fold_Object_BiFunctionContext extends TraversalMethod_foldContext {
  TerminalNode? K_FOLD() => getToken(GremlinParser.TOKEN_K_FOLD, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalBiFunctionContext? traversalBiFunction() => getRuleContext<TraversalBiFunctionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_fold_Object_BiFunctionContext(TraversalMethod_foldContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_fold_Object_BiFunction(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_fold_EmptyContext extends TraversalMethod_foldContext {
  TerminalNode? K_FOLD() => getToken(GremlinParser.TOKEN_K_FOLD, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_fold_EmptyContext(TraversalMethod_foldContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_fold_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_format_StringContext extends TraversalMethod_formatContext {
  TerminalNode? K_FORMAT() => getToken(GremlinParser.TOKEN_K_FORMAT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_format_StringContext(TraversalMethod_formatContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_format_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_from_StringContext extends TraversalMethod_fromContext {
  TerminalNode? K_FROM() => getToken(GremlinParser.TOKEN_K_FROM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_from_StringContext(TraversalMethod_fromContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_from_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_from_TraversalContext extends TraversalMethod_fromContext {
  TerminalNode? K_FROM() => getToken(GremlinParser.TOKEN_K_FROM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_from_TraversalContext(TraversalMethod_fromContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_from_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_group_EmptyContext extends TraversalMethod_groupContext {
  TerminalNode? K_GROUP() => getToken(GremlinParser.TOKEN_K_GROUP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_group_EmptyContext(TraversalMethod_groupContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_group_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_group_StringContext extends TraversalMethod_groupContext {
  TerminalNode? K_GROUP() => getToken(GremlinParser.TOKEN_K_GROUP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_group_StringContext(TraversalMethod_groupContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_group_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_groupCount_StringContext extends TraversalMethod_groupCountContext {
  TerminalNode? K_GROUPCOUNT() => getToken(GremlinParser.TOKEN_K_GROUPCOUNT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_groupCount_StringContext(TraversalMethod_groupCountContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_groupCount_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_groupCount_EmptyContext extends TraversalMethod_groupCountContext {
  TerminalNode? K_GROUPCOUNT() => getToken(GremlinParser.TOKEN_K_GROUPCOUNT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_groupCount_EmptyContext(TraversalMethod_groupCountContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_groupCount_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_has_T_ObjectContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_T_ObjectContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_T_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_String_String_ObjectContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentContext? stringNullableArgument() => getRuleContext<StringNullableArgumentContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_String_String_ObjectContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_String_String_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_StringContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_StringContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_T_PContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalTContext? traversalT() => getRuleContext<TraversalTContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_T_PContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_T_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_String_PContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_String_PContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_String_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_String_ObjectContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_String_ObjectContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_String_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_has_String_String_PContext extends TraversalMethod_hasContext {
  TerminalNode? K_HAS() => getToken(GremlinParser.TOKEN_K_HAS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentContext? stringNullableArgument() => getRuleContext<StringNullableArgumentContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_has_String_String_PContext(TraversalMethod_hasContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_has_String_String_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_hasId_PContext extends TraversalMethod_hasIdContext {
  TerminalNode? K_HASID() => getToken(GremlinParser.TOKEN_K_HASID, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_hasId_PContext(TraversalMethod_hasIdContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasId_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_hasId_Object_ObjectContext extends TraversalMethod_hasIdContext {
  TerminalNode? K_HASID() => getToken(GremlinParser.TOKEN_K_HASID, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalMethod_hasId_Object_ObjectContext(TraversalMethod_hasIdContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasId_Object_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_hasKey_PContext extends TraversalMethod_hasKeyContext {
  TerminalNode? K_HASKEY() => getToken(GremlinParser.TOKEN_K_HASKEY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_hasKey_PContext(TraversalMethod_hasKeyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasKey_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_hasKey_String_StringContext extends TraversalMethod_hasKeyContext {
  TerminalNode? K_HASKEY() => getToken(GremlinParser.TOKEN_K_HASKEY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_hasKey_String_StringContext(TraversalMethod_hasKeyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasKey_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_hasLabel_String_StringContext extends TraversalMethod_hasLabelContext {
  TerminalNode? K_HASLABEL() => getToken(GremlinParser.TOKEN_K_HASLABEL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableArgumentContext? stringNullableArgument() => getRuleContext<StringNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TraversalMethod_hasLabel_String_StringContext(TraversalMethod_hasLabelContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasLabel_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_hasLabel_PContext extends TraversalMethod_hasLabelContext {
  TerminalNode? K_HASLABEL() => getToken(GremlinParser.TOKEN_K_HASLABEL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_hasLabel_PContext(TraversalMethod_hasLabelContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasLabel_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_hasValue_PContext extends TraversalMethod_hasValueContext {
  TerminalNode? K_HASVALUE() => getToken(GremlinParser.TOKEN_K_HASVALUE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_hasValue_PContext(TraversalMethod_hasValueContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasValue_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_hasValue_Object_ObjectContext extends TraversalMethod_hasValueContext {
  TerminalNode? K_HASVALUE() => getToken(GremlinParser.TOKEN_K_HASVALUE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalMethod_hasValue_Object_ObjectContext(TraversalMethod_hasValueContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_hasValue_Object_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_intersect_ObjectContext extends TraversalMethod_intersectContext {
  TerminalNode? K_INTERSECT() => getToken(GremlinParser.TOKEN_K_INTERSECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_intersect_ObjectContext(TraversalMethod_intersectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_intersect_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_is_ObjectContext extends TraversalMethod_isContext {
  TerminalNode? K_IS() => getToken(GremlinParser.TOKEN_K_IS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_is_ObjectContext(TraversalMethod_isContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_is_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_is_PContext extends TraversalMethod_isContext {
  TerminalNode? K_IS() => getToken(GremlinParser.TOKEN_K_IS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_is_PContext(TraversalMethod_isContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_is_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_length_ScopeContext extends TraversalMethod_lengthContext {
  TerminalNode? K_LENGTH() => getToken(GremlinParser.TOKEN_K_LENGTH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_length_ScopeContext(TraversalMethod_lengthContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_length_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_length_EmptyContext extends TraversalMethod_lengthContext {
  TerminalNode? K_LENGTH() => getToken(GremlinParser.TOKEN_K_LENGTH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_length_EmptyContext(TraversalMethod_lengthContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_length_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_limit_Scope_longContext extends TraversalMethod_limitContext {
  TerminalNode? K_LIMIT() => getToken(GremlinParser.TOKEN_K_LIMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_limit_Scope_longContext(TraversalMethod_limitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_limit_Scope_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_limit_longContext extends TraversalMethod_limitContext {
  TerminalNode? K_LIMIT() => getToken(GremlinParser.TOKEN_K_LIMIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_limit_longContext(TraversalMethod_limitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_limit_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_loops_StringContext extends TraversalMethod_loopsContext {
  TerminalNode? K_LOOPS() => getToken(GremlinParser.TOKEN_K_LOOPS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_loops_StringContext(TraversalMethod_loopsContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_loops_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_loops_EmptyContext extends TraversalMethod_loopsContext {
  TerminalNode? K_LOOPS() => getToken(GremlinParser.TOKEN_K_LOOPS, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_loops_EmptyContext(TraversalMethod_loopsContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_loops_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_lTrim_ScopeContext extends TraversalMethod_lTrimContext {
  TerminalNode? K_LTRIM() => getToken(GremlinParser.TOKEN_K_LTRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_lTrim_ScopeContext(TraversalMethod_lTrimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_lTrim_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_lTrim_EmptyContext extends TraversalMethod_lTrimContext {
  TerminalNode? K_LTRIM() => getToken(GremlinParser.TOKEN_K_LTRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_lTrim_EmptyContext(TraversalMethod_lTrimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_lTrim_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_max_ScopeContext extends TraversalMethod_maxContext {
  TerminalNode? K_MAX() => getToken(GremlinParser.TOKEN_K_MAX, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_max_ScopeContext(TraversalMethod_maxContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_max_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_max_EmptyContext extends TraversalMethod_maxContext {
  TerminalNode? K_MAX() => getToken(GremlinParser.TOKEN_K_MAX, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_max_EmptyContext(TraversalMethod_maxContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_max_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_mean_ScopeContext extends TraversalMethod_meanContext {
  TerminalNode? K_MEAN() => getToken(GremlinParser.TOKEN_K_MEAN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mean_ScopeContext(TraversalMethod_meanContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mean_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mean_EmptyContext extends TraversalMethod_meanContext {
  TerminalNode? K_MEAN() => getToken(GremlinParser.TOKEN_K_MEAN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mean_EmptyContext(TraversalMethod_meanContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mean_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_merge_ObjectContext extends TraversalMethod_mergeContext {
  TerminalNode? K_MERGE() => getToken(GremlinParser.TOKEN_K_MERGE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_merge_ObjectContext(TraversalMethod_mergeContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_merge_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_mergeV_MapContext extends TraversalMethod_mergeVContext {
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeV_MapContext(TraversalMethod_mergeVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeV_Map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mergeV_TraversalContext extends TraversalMethod_mergeVContext {
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeV_TraversalContext(TraversalMethod_mergeVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeV_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mergeV_emptyContext extends TraversalMethod_mergeVContext {
  TerminalNode? K_MERGEV() => getToken(GremlinParser.TOKEN_K_MERGEV, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeV_emptyContext(TraversalMethod_mergeVContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeV_empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_mergeE_emptyContext extends TraversalMethod_mergeEContext {
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeE_emptyContext(TraversalMethod_mergeEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeE_empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mergeE_MapContext extends TraversalMethod_mergeEContext {
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeE_MapContext(TraversalMethod_mergeEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeE_Map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_mergeE_TraversalContext extends TraversalMethod_mergeEContext {
  TerminalNode? K_MERGEE() => getToken(GremlinParser.TOKEN_K_MERGEE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_mergeE_TraversalContext(TraversalMethod_mergeEContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_mergeE_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_min_EmptyContext extends TraversalMethod_minContext {
  TerminalNode? K_MIN() => getToken(GremlinParser.TOKEN_K_MIN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_min_EmptyContext(TraversalMethod_minContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_min_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_min_ScopeContext extends TraversalMethod_minContext {
  TerminalNode? K_MIN() => getToken(GremlinParser.TOKEN_K_MIN, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_min_ScopeContext(TraversalMethod_minContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_min_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_none_PContext extends TraversalMethod_noneContext {
  TerminalNode? K_NONE() => getToken(GremlinParser.TOKEN_K_NONE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_none_PContext(TraversalMethod_noneContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_none_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_option_Merge_MapContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalMergeContext? traversalMerge() => getRuleContext<TraversalMergeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_Merge_MapContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Merge_Map(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_option_Merge_Map_CardinalityContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalMergeContext? traversalMerge() => getRuleContext<TraversalMergeContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TraversalCardinalityContext? traversalCardinality() => getRuleContext<TraversalCardinalityContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_Merge_Map_CardinalityContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Merge_Map_Cardinality(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_option_Merge_TraversalContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalMergeContext? traversalMerge() => getRuleContext<TraversalMergeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_Merge_TraversalContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Merge_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_option_Object_TraversalContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_Object_TraversalContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Object_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_option_Predicate_TraversalContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_Predicate_TraversalContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Predicate_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_option_TraversalContext extends TraversalMethod_optionContext {
  TerminalNode? K_OPTION() => getToken(GremlinParser.TOKEN_K_OPTION, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_option_TraversalContext(TraversalMethod_optionContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_option_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_order_EmptyContext extends TraversalMethod_orderContext {
  TerminalNode? K_ORDER() => getToken(GremlinParser.TOKEN_K_ORDER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_order_EmptyContext(TraversalMethod_orderContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_order_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_order_ScopeContext extends TraversalMethod_orderContext {
  TerminalNode? K_ORDER() => getToken(GremlinParser.TOKEN_K_ORDER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_order_ScopeContext(TraversalMethod_orderContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_order_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_pageRank_EmptyContext extends TraversalMethod_pageRankContext {
  TerminalNode? K_PAGERANK() => getToken(GremlinParser.TOKEN_K_PAGERANK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_pageRank_EmptyContext(TraversalMethod_pageRankContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_pageRank_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_pageRank_doubleContext extends TraversalMethod_pageRankContext {
  TerminalNode? K_PAGERANK() => getToken(GremlinParser.TOKEN_K_PAGERANK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NumericLiteralContext? numericLiteral() => getRuleContext<NumericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_pageRank_doubleContext(TraversalMethod_pageRankContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_pageRank_double(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_product_ObjectContext extends TraversalMethod_productContext {
  TerminalNode? K_PRODUCT() => getToken(GremlinParser.TOKEN_K_PRODUCT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_product_ObjectContext(TraversalMethod_productContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_product_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_profile_StringContext extends TraversalMethod_profileContext {
  TerminalNode? K_PROFILE() => getToken(GremlinParser.TOKEN_K_PROFILE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_profile_StringContext(TraversalMethod_profileContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_profile_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_profile_EmptyContext extends TraversalMethod_profileContext {
  TerminalNode? K_PROFILE() => getToken(GremlinParser.TOKEN_K_PROFILE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_profile_EmptyContext(TraversalMethod_profileContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_profile_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_property_Cardinality_Object_Object_ObjectContext extends TraversalMethod_propertyContext {
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalCardinalityContext? traversalCardinality() => getRuleContext<TraversalCardinalityContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalMethod_property_Cardinality_Object_Object_ObjectContext(TraversalMethod_propertyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_property_Cardinality_Object_Object_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_property_Object_Object_ObjectContext extends TraversalMethod_propertyContext {
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  GenericArgumentContext? genericArgument() => getRuleContext<GenericArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  GenericArgumentVarargsContext? genericArgumentVarargs() => getRuleContext<GenericArgumentVarargsContext>(0);
  TraversalMethod_property_Object_Object_ObjectContext(TraversalMethod_propertyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_property_Object_Object_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_property_ObjectContext extends TraversalMethod_propertyContext {
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_property_ObjectContext(TraversalMethod_propertyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_property_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_property_Cardinality_ObjectContext extends TraversalMethod_propertyContext {
  TerminalNode? K_PROPERTY() => getToken(GremlinParser.TOKEN_K_PROPERTY, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalCardinalityContext? traversalCardinality() => getRuleContext<TraversalCardinalityContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  GenericMapNullableArgumentContext? genericMapNullableArgument() => getRuleContext<GenericMapNullableArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_property_Cardinality_ObjectContext(TraversalMethod_propertyContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_property_Cardinality_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_range_Scope_long_longContext extends TraversalMethod_rangeContext {
  TerminalNode? K_RANGE() => getToken(GremlinParser.TOKEN_K_RANGE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  List<IntegerArgumentContext> integerArguments() => getRuleContexts<IntegerArgumentContext>();
  IntegerArgumentContext? integerArgument(int i) => getRuleContext<IntegerArgumentContext>(i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_range_Scope_long_longContext(TraversalMethod_rangeContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_range_Scope_long_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_range_long_longContext extends TraversalMethod_rangeContext {
  TerminalNode? K_RANGE() => getToken(GremlinParser.TOKEN_K_RANGE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<IntegerArgumentContext> integerArguments() => getRuleContexts<IntegerArgumentContext>();
  IntegerArgumentContext? integerArgument(int i) => getRuleContext<IntegerArgumentContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_range_long_longContext(TraversalMethod_rangeContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_range_long_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_repeat_String_TraversalContext extends TraversalMethod_repeatContext {
  TerminalNode? K_REPEAT() => getToken(GremlinParser.TOKEN_K_REPEAT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_repeat_String_TraversalContext(TraversalMethod_repeatContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_repeat_String_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_repeat_TraversalContext extends TraversalMethod_repeatContext {
  TerminalNode? K_REPEAT() => getToken(GremlinParser.TOKEN_K_REPEAT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_repeat_TraversalContext(TraversalMethod_repeatContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_repeat_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_replace_String_StringContext extends TraversalMethod_replaceContext {
  TerminalNode? K_REPLACE() => getToken(GremlinParser.TOKEN_K_REPLACE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<StringNullableLiteralContext> stringNullableLiterals() => getRuleContexts<StringNullableLiteralContext>();
  StringNullableLiteralContext? stringNullableLiteral(int i) => getRuleContext<StringNullableLiteralContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_replace_String_StringContext(TraversalMethod_replaceContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_replace_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_replace_Scope_String_StringContext extends TraversalMethod_replaceContext {
  TerminalNode? K_REPLACE() => getToken(GremlinParser.TOKEN_K_REPLACE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  List<StringNullableLiteralContext> stringNullableLiterals() => getRuleContexts<StringNullableLiteralContext>();
  StringNullableLiteralContext? stringNullableLiteral(int i) => getRuleContext<StringNullableLiteralContext>(i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_replace_Scope_String_StringContext(TraversalMethod_replaceContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_replace_Scope_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_reverse_EmptyContext extends TraversalMethod_reverseContext {
  TerminalNode? K_REVERSE() => getToken(GremlinParser.TOKEN_K_REVERSE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_reverse_EmptyContext(TraversalMethod_reverseContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_reverse_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_rTrim_ScopeContext extends TraversalMethod_rTrimContext {
  TerminalNode? K_RTRIM() => getToken(GremlinParser.TOKEN_K_RTRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_rTrim_ScopeContext(TraversalMethod_rTrimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_rTrim_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_rTrim_EmptyContext extends TraversalMethod_rTrimContext {
  TerminalNode? K_RTRIM() => getToken(GremlinParser.TOKEN_K_RTRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_rTrim_EmptyContext(TraversalMethod_rTrimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_rTrim_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_sack_EmptyContext extends TraversalMethod_sackContext {
  TerminalNode? K_SACK() => getToken(GremlinParser.TOKEN_K_SACK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sack_EmptyContext(TraversalMethod_sackContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sack_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_sack_BiFunctionContext extends TraversalMethod_sackContext {
  TerminalNode? K_SACK() => getToken(GremlinParser.TOKEN_K_SACK, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalBiFunctionContext? traversalBiFunction() => getRuleContext<TraversalBiFunctionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sack_BiFunctionContext(TraversalMethod_sackContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sack_BiFunction(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_sample_Scope_intContext extends TraversalMethod_sampleContext {
  TerminalNode? K_SAMPLE() => getToken(GremlinParser.TOKEN_K_SAMPLE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sample_Scope_intContext(TraversalMethod_sampleContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sample_Scope_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_sample_intContext extends TraversalMethod_sampleContext {
  TerminalNode? K_SAMPLE() => getToken(GremlinParser.TOKEN_K_SAMPLE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sample_intContext(TraversalMethod_sampleContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sample_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_select_String_String_StringContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<StringLiteralContext> stringLiterals() => getRuleContexts<StringLiteralContext>();
  StringLiteralContext? stringLiteral(int i) => getRuleContext<StringLiteralContext>(i);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_select_String_String_StringContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_String_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_Pop_String_String_StringContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPopContext? traversalPop() => getRuleContext<TraversalPopContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  List<StringLiteralContext> stringLiterals() => getRuleContexts<StringLiteralContext>();
  StringLiteralContext? stringLiteral(int i) => getRuleContext<StringLiteralContext>(i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_select_Pop_String_String_StringContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_Pop_String_String_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_StringContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_select_StringContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_Pop_TraversalContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPopContext? traversalPop() => getRuleContext<TraversalPopContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_select_Pop_TraversalContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_Pop_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_TraversalContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_select_TraversalContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_ColumnContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalColumnContext? traversalColumn() => getRuleContext<TraversalColumnContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_select_ColumnContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_Column(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_select_Pop_StringContext extends TraversalMethod_selectContext {
  TerminalNode? K_SELECT() => getToken(GremlinParser.TOKEN_K_SELECT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPopContext? traversalPop() => getRuleContext<TraversalPopContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_select_Pop_StringContext(TraversalMethod_selectContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_select_Pop_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_skip_Scope_longContext extends TraversalMethod_skipContext {
  TerminalNode? K_SKIP() => getToken(GremlinParser.TOKEN_K_SKIP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_skip_Scope_longContext(TraversalMethod_skipContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_skip_Scope_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_skip_longContext extends TraversalMethod_skipContext {
  TerminalNode? K_SKIP() => getToken(GremlinParser.TOKEN_K_SKIP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_skip_longContext(TraversalMethod_skipContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_skip_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_split_StringContext extends TraversalMethod_splitContext {
  TerminalNode? K_SPLIT() => getToken(GremlinParser.TOKEN_K_SPLIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_split_StringContext(TraversalMethod_splitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_split_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_split_Scope_StringContext extends TraversalMethod_splitContext {
  TerminalNode? K_SPLIT() => getToken(GremlinParser.TOKEN_K_SPLIT, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralContext? stringNullableLiteral() => getRuleContext<StringNullableLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_split_Scope_StringContext(TraversalMethod_splitContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_split_Scope_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_substring_int_intContext extends TraversalMethod_substringContext {
  TerminalNode? K_SUBSTRING() => getToken(GremlinParser.TOKEN_K_SUBSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  List<IntegerLiteralContext> integerLiterals() => getRuleContexts<IntegerLiteralContext>();
  IntegerLiteralContext? integerLiteral(int i) => getRuleContext<IntegerLiteralContext>(i);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_substring_int_intContext(TraversalMethod_substringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_substring_int_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_substring_intContext extends TraversalMethod_substringContext {
  TerminalNode? K_SUBSTRING() => getToken(GremlinParser.TOKEN_K_SUBSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_substring_intContext(TraversalMethod_substringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_substring_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_substring_Scope_intContext extends TraversalMethod_substringContext {
  TerminalNode? K_SUBSTRING() => getToken(GremlinParser.TOKEN_K_SUBSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerLiteralContext? integerLiteral() => getRuleContext<IntegerLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_substring_Scope_intContext(TraversalMethod_substringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_substring_Scope_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_substring_Scope_int_intContext extends TraversalMethod_substringContext {
  TerminalNode? K_SUBSTRING() => getToken(GremlinParser.TOKEN_K_SUBSTRING, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  List<TerminalNode> COMMAs() => getTokens(GremlinParser.TOKEN_COMMA);
  TerminalNode? COMMA(int i) => getToken(GremlinParser.TOKEN_COMMA, i);
  List<IntegerLiteralContext> integerLiterals() => getRuleContexts<IntegerLiteralContext>();
  IntegerLiteralContext? integerLiteral(int i) => getRuleContext<IntegerLiteralContext>(i);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_substring_Scope_int_intContext(TraversalMethod_substringContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_substring_Scope_int_int(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_sum_EmptyContext extends TraversalMethod_sumContext {
  TerminalNode? K_SUM() => getToken(GremlinParser.TOKEN_K_SUM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sum_EmptyContext(TraversalMethod_sumContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sum_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_sum_ScopeContext extends TraversalMethod_sumContext {
  TerminalNode? K_SUM() => getToken(GremlinParser.TOKEN_K_SUM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_sum_ScopeContext(TraversalMethod_sumContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_sum_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_tail_longContext extends TraversalMethod_tailContext {
  TerminalNode? K_TAIL() => getToken(GremlinParser.TOKEN_K_TAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tail_longContext(TraversalMethod_tailContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tail_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_tail_EmptyContext extends TraversalMethod_tailContext {
  TerminalNode? K_TAIL() => getToken(GremlinParser.TOKEN_K_TAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tail_EmptyContext(TraversalMethod_tailContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tail_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_tail_Scope_longContext extends TraversalMethod_tailContext {
  TerminalNode? K_TAIL() => getToken(GremlinParser.TOKEN_K_TAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  IntegerArgumentContext? integerArgument() => getRuleContext<IntegerArgumentContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tail_Scope_longContext(TraversalMethod_tailContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tail_Scope_long(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_tail_ScopeContext extends TraversalMethod_tailContext {
  TerminalNode? K_TAIL() => getToken(GremlinParser.TOKEN_K_TAIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tail_ScopeContext(TraversalMethod_tailContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tail_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_to_TraversalContext extends TraversalMethod_toContext {
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_to_TraversalContext(TraversalMethod_toContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_to_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_to_Direction_StringContext extends TraversalMethod_toContext {
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalDirectionContext? traversalDirection() => getRuleContext<TraversalDirectionContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableArgumentVarargsContext? stringNullableArgumentVarargs() => getRuleContext<StringNullableArgumentVarargsContext>(0);
  TraversalMethod_to_Direction_StringContext(TraversalMethod_toContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_to_Direction_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_to_StringContext extends TraversalMethod_toContext {
  TerminalNode? K_TO() => getToken(GremlinParser.TOKEN_K_TO, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_to_StringContext(TraversalMethod_toContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_to_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_toLower_ScopeContext extends TraversalMethod_toLowerContext {
  TerminalNode? K_TOLOWER() => getToken(GremlinParser.TOKEN_K_TOLOWER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_toLower_ScopeContext(TraversalMethod_toLowerContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toLower_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_toLower_EmptyContext extends TraversalMethod_toLowerContext {
  TerminalNode? K_TOLOWER() => getToken(GremlinParser.TOKEN_K_TOLOWER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_toLower_EmptyContext(TraversalMethod_toLowerContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toLower_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_toUpper_ScopeContext extends TraversalMethod_toUpperContext {
  TerminalNode? K_TOUPPER() => getToken(GremlinParser.TOKEN_K_TOUPPER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_toUpper_ScopeContext(TraversalMethod_toUpperContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toUpper_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_toUpper_EmptyContext extends TraversalMethod_toUpperContext {
  TerminalNode? K_TOUPPER() => getToken(GremlinParser.TOKEN_K_TOUPPER, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_toUpper_EmptyContext(TraversalMethod_toUpperContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_toUpper_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_tree_StringContext extends TraversalMethod_treeContext {
  TerminalNode? K_TREE() => getToken(GremlinParser.TOKEN_K_TREE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tree_StringContext(TraversalMethod_treeContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tree_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_tree_EmptyContext extends TraversalMethod_treeContext {
  TerminalNode? K_TREE() => getToken(GremlinParser.TOKEN_K_TREE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_tree_EmptyContext(TraversalMethod_treeContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_tree_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_trim_EmptyContext extends TraversalMethod_trimContext {
  TerminalNode? K_TRIM() => getToken(GremlinParser.TOKEN_K_TRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_trim_EmptyContext(TraversalMethod_trimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_trim_Empty(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_trim_ScopeContext extends TraversalMethod_trimContext {
  TerminalNode? K_TRIM() => getToken(GremlinParser.TOKEN_K_TRIM, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalScopeContext? traversalScope() => getRuleContext<TraversalScopeContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_trim_ScopeContext(TraversalMethod_trimContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_trim_Scope(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_until_TraversalContext extends TraversalMethod_untilContext {
  TerminalNode? K_UNTIL() => getToken(GremlinParser.TOKEN_K_UNTIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_until_TraversalContext(TraversalMethod_untilContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_until_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_until_PredicateContext extends TraversalMethod_untilContext {
  TerminalNode? K_UNTIL() => getToken(GremlinParser.TOKEN_K_UNTIL, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_until_PredicateContext(TraversalMethod_untilContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_until_Predicate(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_valueMap_StringContext extends TraversalMethod_valueMapContext {
  TerminalNode? K_VALUEMAP() => getToken(GremlinParser.TOKEN_K_VALUEMAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_valueMap_StringContext(TraversalMethod_valueMapContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_valueMap_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_valueMap_boolean_StringContext extends TraversalMethod_valueMapContext {
  TerminalNode? K_VALUEMAP() => getToken(GremlinParser.TOKEN_K_VALUEMAP, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  BooleanLiteralContext? booleanLiteral() => getRuleContext<BooleanLiteralContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  StringNullableLiteralVarargsContext? stringNullableLiteralVarargs() => getRuleContext<StringNullableLiteralVarargsContext>(0);
  TraversalMethod_valueMap_boolean_StringContext(TraversalMethod_valueMapContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_valueMap_boolean_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_where_PContext extends TraversalMethod_whereContext {
  TerminalNode? K_WHERE() => getToken(GremlinParser.TOKEN_K_WHERE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_where_PContext(TraversalMethod_whereContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_where_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_where_String_PContext extends TraversalMethod_whereContext {
  TerminalNode? K_WHERE() => getToken(GremlinParser.TOKEN_K_WHERE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TraversalPredicateContext? traversalPredicate() => getRuleContext<TraversalPredicateContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_where_String_PContext(TraversalMethod_whereContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_where_String_P(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_where_TraversalContext extends TraversalMethod_whereContext {
  TerminalNode? K_WHERE() => getToken(GremlinParser.TOKEN_K_WHERE, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  NestedTraversalContext? nestedTraversal() => getRuleContext<NestedTraversalContext>(0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  TraversalMethod_where_TraversalContext(TraversalMethod_whereContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_where_Traversal(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}class TraversalMethod_with_StringContext extends TraversalMethod_withContext {
  TerminalNode? K_WITH() => getToken(GremlinParser.TOKEN_K_WITH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  WithOptionKeysContext? withOptionKeys() => getRuleContext<WithOptionKeysContext>(0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  TraversalMethod_with_StringContext(TraversalMethod_withContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_with_String(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}

class TraversalMethod_with_String_ObjectContext extends TraversalMethod_withContext {
  TerminalNode? K_WITH() => getToken(GremlinParser.TOKEN_K_WITH, 0);
  TerminalNode? LPAREN() => getToken(GremlinParser.TOKEN_LPAREN, 0);
  TerminalNode? COMMA() => getToken(GremlinParser.TOKEN_COMMA, 0);
  TerminalNode? RPAREN() => getToken(GremlinParser.TOKEN_RPAREN, 0);
  WithOptionKeysContext? withOptionKeys() => getRuleContext<WithOptionKeysContext>(0);
  StringLiteralContext? stringLiteral() => getRuleContext<StringLiteralContext>(0);
  WithOptionsValuesContext? withOptionsValues() => getRuleContext<WithOptionsValuesContext>(0);
  IoOptionsValuesContext? ioOptionsValues() => getRuleContext<IoOptionsValuesContext>(0);
  GenericLiteralContext? genericLiteral() => getRuleContext<GenericLiteralContext>(0);
  TraversalMethod_with_String_ObjectContext(TraversalMethod_withContext ctx) { copyFrom(ctx); }
  @override
  T? accept<T>(ParseTreeVisitor<T> visitor) {
    if (visitor is GremlinVisitor<T>) {
     return visitor.visitTraversalMethod_with_String_Object(this);
    } else {
    	return visitor.visitChildren(this);
    }
  }
}