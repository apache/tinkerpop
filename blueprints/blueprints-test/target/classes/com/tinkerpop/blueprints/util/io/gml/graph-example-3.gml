graph [
	node [
		id 1
		id2 2
		name "marko"
		age 29
	]
	node [
		id 2
		id2 3
		name "vadas"
		age 27
	]
	node [
		id 3
		id2 4
		name "lop"
		lang "java"
	]
	node [
		id 4
		id2 5
		name "josh"
		age 32
	]
	node [
		id 5
		id2 7
		name "ripple"
		lang "java"
	]
	node [
		id 6
		name "peter"
		age 35
	]
	edge [
		id 7
		source 1
		target 2
		label "knows"
		weight 0.5
		id2 8
		label2 "has high fived"
	]
	edge [
		id 8
		source 1
		target 4
		label "knows"
		weight 1.0
		id2 9
		label2 "has high fived"
	]
	edge [
		id 9
		source 1
		target 3
		label "created"
		weight 0.4
		id2 10
		label2 "has high fived"
	]
	edge [
		id 10
		source 4
		target 5
		label "created"
		weight 1.0
		id2 11
		label2 "has high fived"
	]
	edge [
		id 11
		source 4
		target 3
		label "created"
		weight 0.4
		id2 13
	]
	edge [
		id 12
		source 6
		target 3
		label "created"
		weight 0.2
	]
]
