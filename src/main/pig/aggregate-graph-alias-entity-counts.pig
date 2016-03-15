SET default_parallel 200;

--------LOAD FEAT------------------

--load feat 
FEAT = LOAD '$feat' USING PigStorage('\t') AS (
	alias:chararray,
	phrase_freq:long,
	alias_freq:long,
	entity_id:chararray, 
	alias_entity_freq:long
);

--------PROCESS INFO------------------

--validate feat
FEAT = FILTER FEAT BY 
	(SIZE(alias) > 0)		AND 
	(SIZE(entity_id) > 0)	AND
	(alias_freq > 0)		AND
	(phrase_freq > 0)		AND
	(alias_entity_freq > 0);

--normalize alias
FEAT = FOREACH FEAT
{
	NORM = com.yahoo.semsearch.fastlinking.udf.NormalizeText(alias, 'true');

	GENERATE
	alias				AS ori_alias,
	NORM				AS alias,
	alias_freq			AS alias_freq,
	phrase_freq			AS phrase_freq, 
	entity_id			AS entity_id,
	alias_entity_freq	AS alias_entity_freq;
}

--vlaidate alias
FEAT = FILTER FEAT BY 
	(SIZE(ori_alias) > 0) AND
	(SIZE(alias) > 0);

--unique vector
FEAT = DISTINCT FEAT;

--------COMPUTE ALIAS-ENTITY COUNTS------------------

ALIAS_ENTITY = GROUP FEAT BY (entity_id, alias);

ALIAS_ENTITY = FOREACH ALIAS_ENTITY 
{ 
	GENERATE
	FLATTEN(group)					AS (entity_id, alias),
	SUM(FEAT.alias_entity_freq)		AS alias_entity_freq;
}


--------COMPUTE ALIAS COUNTS------------------

ALIAS = GROUP ALIAS_ENTITY BY (alias);

ALIAS = FOREACH ALIAS 
{ 
	GENERATE
	group									AS alias,
	SUM(ALIAS_ENTITY.alias_entity_freq)		AS alias_freq;
}

--------COMPUTE PHRASE COUNTS------------------

PHRASE = FOREACH FEAT
{
	GENERATE
	ori_alias,
	alias,
	phrase_freq;
}

PHRASE = DISTINCT PHRASE;

GROUP_PHRASE = GROUP PHRASE BY (alias);

PHRASE = FOREACH GROUP_PHRASE 
{ 
	GENERATE
	group					AS alias,
	SUM(PHRASE.phrase_freq)	AS phrase_freq;
}


--------COMPUTE ENTITY COUNTS------------------

ENTITY = GROUP ALIAS_ENTITY BY (entity_id);

ENTITY = FOREACH ENTITY 
{ 
	GENERATE
	FLATTEN(group)							AS (entity_id),
	SUM(ALIAS_ENTITY.alias_entity_freq)		AS entity_freq;
}

--------PROPAGATE COUNTS------------------

FEAT = JOIN PHRASE	BY (alias),			ALIAS			BY (alias);
FEAT = JOIN FEAT	BY (PHRASE::alias),	ALIAS_ENTITY	BY (alias);
FEAT = JOIN ENTITY	BY (entity_id),		FEAT			BY (ALIAS_ENTITY::entity_id);
FEAT = FOREACH FEAT
{
	GENERATE
	ALIAS::alias						AS alias,
	ALIAS::alias_freq					AS alias_freq,
	PHRASE::phrase_freq					AS phrase_freq,
	ENTITY::entity_id					AS entity_id,
	ENTITY::entity_freq					AS entity_freq,
	ALIAS_ENTITY::alias_entity_freq		AS alias_entity_freq;
}

STORE FEAT INTO '$output' USING PigStorage('\t');
