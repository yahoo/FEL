SET default_parallel 200;

--------LOAD FEATURES------------------

FEAT = LOAD '$counts' USING PigStorage('\t') AS
(
	alias:chararray,
	alias_freq:long,
	phrase_freq:long,
	entity_id:chararray,
	entity_freq:long,
	alias_entity_freq:long	
);

FEAT = FILTER FEAT BY 
	(SIZE(alias) > 0)		AND 
	(SIZE(entity_id) > 0)	AND
	(alias_freq > 0)		AND
	(phrase_freq > 0)		AND
	(entity_freq > 0)		AND
	(alias_entity_freq > 0);

FEAT = DISTINCT FEAT;

--------NORMALIZE COUNT ACROSS ENTITY ID (YK)-----------

ALIAS_ENTITY = GROUP FEAT BY (entity_id, alias);
ALIAS_ENTITY = FOREACH ALIAS_ENTITY 
{ 
	GENERATE
	FLATTEN(group)					AS (entity_id, alias),
	SUM(FEAT.alias_entity_freq)		AS alias_entity_freq;
}

ALIAS = GROUP ALIAS_ENTITY BY (alias);
ALIAS = FOREACH ALIAS 
{ 
	GENERATE
	group									AS alias,
	SUM(ALIAS_ENTITY.alias_entity_freq)		AS alias_freq;
}

PHRASE = GROUP FEAT BY (alias);
PHRASE = FOREACH PHRASE
{
	ALIAS_FREQ		= SUM(FEAT.alias_entity_freq);
	ALIAS_MAX		= MAX(FEAT.alias_freq);	
	
	COEFF			= (double) ALIAS_FREQ / (double) ALIAS_MAX;
	
	PHRASE_FREQ_00	= MAX(FEAT.phrase_freq);	 
	PHRASE_FREQ_01	= COEFF * (double) PHRASE_FREQ_00;
	PHRASE_FREQ		= CEIL(PHRASE_FREQ_01);

	GENERATE
	group					AS alias,
	(long) PHRASE_FREQ		AS	phrase_freq;
}

ENTITY = GROUP ALIAS_ENTITY BY (entity_id);
ENTITY = FOREACH ENTITY 
{ 
	GENERATE
	FLATTEN(group)							AS (entity_id),
	SUM(ALIAS_ENTITY.alias_entity_freq)		AS entity_freq;
}

FEAT = JOIN PHRASE BY (alias), ALIAS BY (alias);
FEAT = JOIN FEAT BY (PHRASE::alias), ALIAS_ENTITY BY (alias);
FEAT = JOIN ENTITY BY (entity_id), FEAT BY (ALIAS_ENTITY::entity_id);
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

FEAT = DISTINCT FEAT;

--------COMPUTE TOTAL COUNTS------------------

ALIAS = FOREACH FEAT
{
	
	GENERATE
	alias,
	alias_freq,
	phrase_freq;
}

ALIAS = DISTINCT ALIAS;

ALIAS_TOTAL = GROUP ALIAS ALL;
ALIAS_TOTAL = FOREACH ALIAS_TOTAL
{
	GENERATE
	SUM(ALIAS.alias_freq)	AS alias_total,
	SUM(ALIAS.phrase_freq)	AS phrase_total;
}

ENTITY = FOREACH FEAT
{
	GENERATE
	entity_id,
	entity_freq;
}

ENTITY = DISTINCT ENTITY;

ENTITY_TOTAL = GROUP ENTITY ALL;
ENTITY_TOTAL = FOREACH ENTITY_TOTAL
{
	GENERATE
	SUM(ENTITY.entity_freq)	AS entity_total;
}

ALIAS_ENTITY = FOREACH FEAT
{
	GENERATE
	alias,
	entity_id,
	alias_entity_freq;
}
ALIAS_ENTITY = DISTINCT ALIAS_ENTITY;

ALIAS_ENTITY_TOTAL = GROUP ALIAS_ENTITY ALL;
ALIAS_ENTITY_TOTAL = FOREACH ALIAS_ENTITY_TOTAL
{
	GENERATE
	SUM(ALIAS_ENTITY.alias_entity_freq)	AS alias_entity_total;
}

TEMP_FEAT = CROSS ALIAS_TOTAL, FEAT;
TEMP_FEAT = CROSS ENTITY_TOTAL, TEMP_FEAT;
TEMP_FEAT = CROSS ALIAS_ENTITY_TOTAL, TEMP_FEAT;

FEAT = FOREACH TEMP_FEAT
{
	GENERATE
	alias,
	alias_freq,
	phrase_freq,
	entity_id,
	entity_freq,
	alias_entity_freq;
}

STORE FEAT INTO '$output' USING PigStorage('\t');
