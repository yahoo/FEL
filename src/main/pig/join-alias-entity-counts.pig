SET default_parallel 200;

ENTITIES = LOAD '$entity' USING PigStorage('\t') AS 
(
	entity:chararray,
	entity_id:chararray
);

ENTITIES = FILTER ENTITIES BY 
	(SIZE(entity_id) > 0)	AND
	(SIZE(entity) > 0);

ENTITIES = DISTINCT ENTITIES;



GRAPH_ALIASES_ENTITIES = LOAD '$graph' USING PigStorage('\t') AS
(
	alias:chararray,
	alias_freq:long,
	alias_total:long,
	entity:chararray,
	entity_freq:long,
	alias_entity_freq:long	
);

GRAPH_ALIASES_ENTITIES = FILTER GRAPH_ALIASES_ENTITIES BY 
	(SIZE(alias) > 0)		AND 
	(SIZE(entity) > 0)		AND
	(alias_freq > 0)		AND
	(alias_total > 0)		AND
	(entity_freq > 0)		AND
	(alias_entity_freq > 0);

GRAPH_ALIASES_ENTITIES = DISTINCT GRAPH_ALIASES_ENTITIES;


GRAPH_ENTITIES = FOREACH GRAPH_ALIASES_ENTITIES
{
	GENERATE
	entity,
	entity_freq;
}

GRAPH_ENTITIES = DISTINCT GRAPH_ENTITIES;


GRAPH_ALIASES = FOREACH GRAPH_ALIASES_ENTITIES
{
	GENERATE
	alias,
	alias_freq,
	alias_total;
}

GRAPH_ALIASES = DISTINCT GRAPH_ALIASES;


ALL_CAND_ENTITIES	= FOREACH GRAPH_ENTITIES { GENERATE entity; }
ALL_CAND_ENTITIES 	= DISTINCT ALL_CAND_ENTITIES;

TEMP_ENTITIES	= JOIN ALL_CAND_ENTITIES	BY (entity)	LEFT OUTER,	ENTITIES	BY (entity);
TEMP_ENTITIES	= FOREACH TEMP_ENTITIES
{
	ENTITY_ID	= (ENTITIES::entity_id	is not null ? ENTITIES::entity_id	: '0');
	TYPE_ID		= '1';
	TYPE		= 'Entity';

	GENERATE
	ENTITY_ID					AS entity_id,
	ALL_CAND_ENTITIES::entity	AS entity,
	TYPE_ID						AS type_id,
	TYPE						AS type;	
}

ALL_ENTITIES	= GROUP TEMP_ENTITIES BY (entity);
ALL_ENTITIES	= FOREACH ALL_ENTITIES
{
	CANDS	= ORDER TEMP_ENTITIES BY entity_id ASC;
	TOP		= LIMIT CANDS 1;

	GENERATE
	FLATTEN(TOP)	AS (entity_id, entity, type_id, type);
} 

ALL_ENTITIES = FILTER ALL_ENTITIES BY
	(entity_id != '0');

STATS_ENTITIES = JOIN ALL_ENTITIES		BY (entity)		LEFT OUTER, GRAPH_ENTITIES	BY (entity);
STATS_ENTITIES = FOREACH STATS_ENTITIES
{
	ENTITY_ID			= ALL_ENTITIES::entity_id;
	ENTITY				= ALL_ENTITIES::entity;
	TYPE_ID				= ALL_ENTITIES::type_id;
	TYPE				= ALL_ENTITIES::type;

	SEARCH_FREQ			= 0L;
	ALT_SEARCH_FREQ		= 0L;
	GRAPH_FREQ			= (GRAPH_ENTITIES::entity_freq is not null	? GRAPH_ENTITIES::entity_freq	: 0L);

	GENERATE
	ENTITY_ID			AS entity_id,
	ENTITY				AS entity,
	TYPE_ID				AS type_id,
	TYPE				AS type,
	SEARCH_FREQ			AS search_entity_freq,
	ALT_SEARCH_FREQ		AS alt_search_entity_freq,
	GRAPH_FREQ			AS graph_entity_freq;
}

STATS_ENTITIES = DISTINCT STATS_ENTITIES;


ALL_ALIASES		= FOREACH GRAPH_ALIASES	{ GENERATE alias; }
ALL_ALIASES 	= DISTINCT ALL_ALIASES;

STATS_ALIASES = JOIN ALL_ALIASES	BY (alias)				LEFT OUTER, GRAPH_ALIASES		BY (alias);
STATS_ALIASES = FOREACH STATS_ALIASES
{
	ALIAS				= ALL_ALIASES::alias;	

	SEARCH_FREQ			= 0L;
	SEARCH_SUM			= 0L;
	SEARCH_TOTAL		= 0L;

	ALT_SEARCH_FREQ		= 0L;
	ALT_SEARCH_TOTAL	= 0L;
	
	GRAPH_FREQ			= (GRAPH_ALIASES::alias_freq is not null	? GRAPH_ALIASES::alias_freq		: 0L);
	GRAPH_TOTAL			= (GRAPH_ALIASES::alias_total is not null	? GRAPH_ALIASES::alias_total	: 0L);

	GENERATE
	ALIAS				AS alias,
	SEARCH_FREQ			AS search_alias_freq,
	SEARCH_SUM			AS search_alias_sum,
	SEARCH_TOTAL		AS search_alias_total,	
	ALT_SEARCH_FREQ		AS alt_search_alias_freq,
	ALT_SEARCH_TOTAL	AS alt_search_alias_total,	
	GRAPH_FREQ			AS graph_alias_freq,
	GRAPH_TOTAL			AS graph_alias_total;	
}

STATS_ALIASES = DISTINCT STATS_ALIASES;


ALL_ALIASES_ENTITIES	= FOREACH GRAPH_ALIASES_ENTITIES { GENERATE alias, entity; }
ALL_ALIASES_ENTITIES 	= DISTINCT ALL_ALIASES_ENTITIES;

STATS_ALIASES_ENTITIES = JOIN ALL_ALIASES_ENTITIES		BY (alias, entity)												LEFT OUTER, GRAPH_ALIASES_ENTITIES	BY (alias, entity);
STATS_ALIASES_ENTITIES = FOREACH STATS_ALIASES_ENTITIES
{
	ALIAS				= ALL_ALIASES_ENTITIES::alias;	
	ENTITY				= ALL_ALIASES_ENTITIES::entity;

	SEARCH_FREQ			= 0L;
	ALT_SEARCH_FREQ		= 0L;
	GRAPH_FREQ			= (GRAPH_ALIASES_ENTITIES::alias_entity_freq is not null	? GRAPH_ALIASES_ENTITIES::alias_entity_freq		: 0L);

	GENERATE
	ALIAS				AS alias,
	ENTITY				AS entity,
	SEARCH_FREQ			AS search_alias_entity_freq,
	ALT_SEARCH_FREQ		AS alt_search_alias_entity_freq,
	GRAPH_FREQ			AS graph_alias_entity_freq;
}
	
STATS_ALIASES_ENTITIES = DISTINCT STATS_ALIASES_ENTITIES;


TEMP_ALIASES_ENTITIES = GROUP STATS_ALIASES_ENTITIES BY (alias);
STATS_ALIASES_ENTITIES = FOREACH TEMP_ALIASES_ENTITIES
{
	ENTITY = DISTINCT STATS_ALIASES_ENTITIES.entity;
	
	GENERATE
	FLATTEN(STATS_ALIASES_ENTITIES)	AS (alias, entity, search_alias_entity_freq, alt_search_alias_entity_freq, graph_alias_entity_freq),
	COUNT(ENTITY)					AS entity_total;
}


STATS = JOIN STATS_ENTITIES BY (entity),	STATS_ALIASES_ENTITIES	BY (entity);
STATS = JOIN STATS_ALIASES	BY (alias),		STATS					BY (alias);
STATS = FOREACH STATS
{
	ALIAS							= STATS_ALIASES::alias;
	ENTITY_ID						= STATS_ENTITIES::entity_id;
	ENTITY							= STATS_ENTITIES::entity;
	TYPE_ID							= STATS_ENTITIES::type_id;
	TYPE							= STATS_ENTITIES::type;

	SEARCH_PROB_PHRASE				= (search_alias_total + 1.0)		/ (double) (search_alias_total + graph_alias_total + 2.0);
	SEARCH_PROB_ALIAS				= (search_alias_sum + 1.0)			/ (double) (search_alias_total + 2.0);
	SEARCH_PROB_ENTITY_ALIAS		= (search_alias_entity_freq + 1.0)	/ (double) (search_alias_freq + entity_total); 
	
	ALT_SEARCH_PROB_ALIAS			= (alt_search_alias_freq + 1.0)			/ (double) (alt_search_alias_total + 2.0);
	ALT_SEARCH_PROB_ENTITY_ALIAS	= (alt_search_alias_entity_freq + 1.0)	/ (double) (alt_search_alias_freq + entity_total);

	GRAPH_PROB_PHRASE				= 1.0 - SEARCH_PROB_PHRASE;
	GRAPH_PROB_ALIAS				= (graph_alias_freq + 1.0)			/ (double) (graph_alias_total + 2.0);
	GRAPH_PROB_ENTITY_ALIAS			= (graph_alias_entity_freq + 1.0)	/ (double) (graph_alias_freq + entity_total);
		
	WEIGHT_SEARCH_PHRASE			= (search_alias_total + 1.0) / (double) (search_alias_total + alt_search_alias_total + graph_alias_total + 3.0);
	WEIGHT_ALT_SEARCH_PHRASE		= (alt_search_alias_total + 1.0) / (double) (search_alias_total + alt_search_alias_total + graph_alias_total + 3.0);
	WEIGHT_GRAPH_PHRASE				= 1.0 - WEIGHT_SEARCH_PHRASE - WEIGHT_ALT_SEARCH_PHRASE;
	
	LINK_IMP_00						= (SEARCH_PROB_PHRASE * SEARCH_PROB_ALIAS * SEARCH_PROB_ENTITY_ALIAS) + (double) (GRAPH_PROB_PHRASE * GRAPH_PROB_ALIAS *  GRAPH_PROB_ENTITY_ALIAS);
	LINK_IMP_01						= (SEARCH_PROB_PHRASE * GRAPH_PROB_ALIAS * SEARCH_PROB_ENTITY_ALIAS) + (double) (GRAPH_PROB_PHRASE * GRAPH_PROB_ALIAS *  GRAPH_PROB_ENTITY_ALIAS);	
	LINK_IMP_02						= (WEIGHT_SEARCH_PHRASE * SEARCH_PROB_ALIAS * SEARCH_PROB_ENTITY_ALIAS) + (WEIGHT_ALT_SEARCH_PHRASE * ALT_SEARCH_PROB_ALIAS * ALT_SEARCH_PROB_ENTITY_ALIAS) + (double) (WEIGHT_GRAPH_PHRASE * GRAPH_PROB_ALIAS * GRAPH_PROB_ENTITY_ALIAS);

	GENERATE
	ALIAS													AS alias,

	STATS_ALIASES::search_alias_freq						AS search_alias_freq,
	STATS_ALIASES::search_alias_sum							AS search_alias_sum,
	STATS_ALIASES::search_alias_total						AS search_alias_total,

	STATS_ALIASES::alt_search_alias_freq					AS alt_search_alias_freq,
	STATS_ALIASES::alt_search_alias_total					AS alt_search_alias_total,

	STATS_ALIASES::graph_alias_freq							AS graph_alias_freq,
	STATS_ALIASES::graph_alias_total						AS graph_alias_total,

	STATS_ALIASES_ENTITIES::entity_total					AS entity_total,

	ENTITY_ID												AS entity_id,
	ENTITY													AS entity,
	TYPE_ID													AS type_id,
	TYPE													AS type,

	STATS_ENTITIES::search_entity_freq						AS search_entity_freq,
	STATS_ALIASES_ENTITIES::search_alias_entity_freq		AS search_alias_entity_freq,

	STATS_ENTITIES::alt_search_entity_freq					AS alt_search_entity_freq,
	STATS_ALIASES_ENTITIES::alt_search_alias_entity_freq	AS alt_search_alias_entity_freq,

	STATS_ENTITIES::graph_entity_freq						AS graph_entity_freq,
	STATS_ALIASES_ENTITIES::graph_alias_entity_freq			AS graph_alias_entity_freq,

	SEARCH_PROB_PHRASE										AS search_prob_phrase,
	SEARCH_PROB_ALIAS										AS search_prob_alias,
	SEARCH_PROB_ENTITY_ALIAS								AS search_prob_entity_alias,

	ALT_SEARCH_PROB_ALIAS									AS alt_search_prob_alias,
	ALT_SEARCH_PROB_ENTITY_ALIAS							AS alt_search_prob_entity_alias,

	GRAPH_PROB_PHRASE										AS graph_prob_phrase,
	GRAPH_PROB_ALIAS										AS graph_prob_alias,
	GRAPH_PROB_ENTITY_ALIAS									AS graph_prob_entity_alias,

	LINK_IMP_00												AS link_importance_00,
	LINK_IMP_01												AS link_importance_01,
	LINK_IMP_02												AS link_importance_02;	
}

STATS = DISTINCT STATS;

FEATURES = FOREACH STATS
{
	ALIAS_00	= CONCAT(	'\u0001',								(chararray) entity_total);
	ALIAS_01	= CONCAT(	CONCAT('\u0001', (chararray) graph_alias_total),		ALIAS_00);
	ALIAS_02	= CONCAT(	CONCAT('\u0001', (chararray) graph_alias_freq),			ALIAS_01);
	ALIAS_03	= CONCAT(	CONCAT('\u0001', (chararray) alt_search_alias_total),	ALIAS_02);
	ALIAS_04	= CONCAT(	CONCAT('\u0001', (chararray) alt_search_alias_freq),	ALIAS_03);
	ALIAS_05	= CONCAT(	CONCAT('\u0001', (chararray) search_alias_total),		ALIAS_04);
	ALIAS_06	= CONCAT(	CONCAT('\u0001', (chararray) search_alias_sum),			ALIAS_05);
	ALIAS_07	= CONCAT(	CONCAT('\u0001', (chararray) search_alias_freq),		ALIAS_06);
	ALIAS		= CONCAT(	(chararray) alias,										ALIAS_07);
	
	ENTITY_00	= CONCAT(	'\u0001',						  (chararray) graph_alias_entity_freq);
	ENTITY_01	= CONCAT(	CONCAT('\u0001', (chararray) graph_entity_freq),			ENTITY_00);
	ENTITY_02	= CONCAT(	CONCAT('\u0001', (chararray) alt_search_alias_entity_freq), ENTITY_01);
	ENTITY_03	= CONCAT(	CONCAT('\u0001', (chararray) alt_search_entity_freq),		ENTITY_02);
	ENTITY_04	= CONCAT(	CONCAT('\u0001', (chararray) search_alias_entity_freq), 	ENTITY_03);
	ENTITY_05	= CONCAT(	CONCAT('\u0001', (chararray) search_entity_freq),			ENTITY_04);
	ENTITY_06	= CONCAT(	CONCAT('\u0001', (chararray) type_id),						ENTITY_05);
	ENTITY		= CONCAT(	(chararray) entity_id, 										ENTITY_06);
	
	GENERATE
	ALIAS		AS alias,
	ENTITY		AS entity;
}

FEATURES = DISTINCT FEATURES;

TEMP_FEATURES	= GROUP FEATURES BY (alias);
FEATURES		= FOREACH TEMP_FEATURES
{
	GENERATE
	group			AS	alias,
	FEATURES.entity	AS entities;
}


STORE ALL_ENTITIES	INTO '$err_output/entity'	USING PigStorage('\t');
STORE STATS			INTO '$err_output/stat'		USING PigStorage('\t');
STORE FEATURES		INTO '$output'				USING PigStorage('\t');
