graph [
	node [
		id 3
		name "lop"
		lang "java"
	]
	node [
		id 2
		age 27
		name "vadas"
	]
	node [
		id 1
		age 29
		name "marko"
	]
	node [
		id 6
		age 35
		name "peter"
	]
	node [
		id 5
		name "ripple"
		lang "java"
	]
	node [
		id 4
		age 32
		name "josh"
	]
	edge [
		source 4
		target 5
		label "created"
		weight 1.0
	]
	edge [
		source 1
		target 2
		label "knows"
		weight 0.5
	]
	edge [
		source 1
		target 3
		label "created"
		weight 0.4
	]
	edge [
		source 1
		target 4
		label "knows"
		weight 1.0
	]
	edge [
		source 4
		target 3
		label "created"
		weight 0.4
	]
	edge [
		source 6
		target 3
		label "created"
		weight 0.2
	]
]
