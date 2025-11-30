
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"

const bool DEBUGBASECASE = true;

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables) {
	MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
	map <string, MyDB_TablePtr> filteredTables = map<string, MyDB_TablePtr>();

	for (auto &val : this->valuesToSelect) {
		// TODO: figure out how we get the actual att type instead of hard coding string
		pair<string, MyDB_AttTypePtr> attribute = make_pair(val->getId(), make_shared<MyDB_StringAttType>());
		cout << "about to add in attribute " << attribute.first << flush << "\n";
		totSchema->appendAtt(attribute);
	}

	// filter the tables to only the ones we are using
	for (auto &[tableName, aliasName] : this->tablesToProcess) {
		cout << "about to add in table " << tableName << flush << "\n";
		MyDB_TablePtr aliasedTable = allTables[tableName]->alias(aliasName);
		filteredTables.insert({aliasName, aliasedTable});
	}

	
	cout << "about to optimize the query plan\n" << flush;
	// here we call the recursive, exhaustive enum. algorithm
	return optimizeQueryPlan (filteredTables, totSchema, this->allDisjunctions);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables, 
	MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &allDisjunctions) {
	double best = 9e99;
	LogicalOpPtr res = nullptr;
	double cost = 9e99;

	// some code here...
	string outputTableName = "tempTable1";
	MyDB_TablePtr outputTable = make_shared<MyDB_Table>(outputTableName, outputTableName, totSchema);
	if (DEBUGBASECASE) {
		cout << "Made the output table\n" << flush;
		cout << "allTables size: " << allTables.size () << " \n" << flush ;
	}

	// case where no joins
	if (allTables.size () == 1) {
		
		MyDB_TablePtr table;
		for (const auto &[key, tablePtr]: allTables) {
			if (DEBUGBASECASE) {
				cout << "at the base case for table " << key  << "\n" << flush;
			}
			table = tablePtr;
		}
		res = make_shared<LogicalTableScan>(
			table, 
			outputTable, 
			make_shared<MyDB_Stats>(table)->costSelection(allDisjunctions), 
			allDisjunctions);
		
		if (DEBUGBASECASE) {
			auto atts = table->getSchema()->getAtts();
			for (auto &[attName, _] : atts) {
				cout << "attribute of name " << attName  << "\n" << flush;
			}
		}
		MyDB_StatsPtr stats = res->getStats();
		MyDB_StatsPtr curCost = stats->costSelection(allDisjunctions);

		// some code here...
		return make_pair (res, curCost->getTupleCount());
	}

	// we have at least one join
	// some code here...
	return make_pair (res, best);
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
