
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"

const bool DEBUGBASECASE = true;
const bool DEBUG_OUTER_FN = true;

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables) {
	MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
	map<string, MyDB_TablePtr> filteredTables = map<string, MyDB_TablePtr>();
	map<string, MyDB_AttTypePtr> attToType = map<string, MyDB_AttTypePtr>();

	if (DEBUG_OUTER_FN) {
		cout << "Going through all disjunctions" << flush << "\n";
		for (auto &d : this->allDisjunctions) {
			cout << "Disjunction: " << d->toString() << "\n" << flush;
		}
	}

	// filter the tables to only the ones we are using
	for (auto &[tableName, aliasName] : this->tablesToProcess) {
		cout << "about to add in table " << tableName << flush << "\n";
		MyDB_TablePtr aliasedTable = allTables[tableName]->alias(aliasName);

		// go through each aliased attribute in this table
		for (auto &att:  aliasedTable->getSchema()->getAtts()) {
			attToType.insert({att.first , att.second});
		}
		filteredTables.insert({aliasName, aliasedTable});
	}

	for (auto &val : this->valuesToSelect) {
		string attName = val->getId();
		cout << "we're at value with id " << attName << "\n" << flush ;
		MyDB_AttTypePtr attType;

		// check if its an agg type
		if (val->isAvg() || val->isSum()) {
			continue;
		}
			// attType = make_shared<MyDB_DoubleAttType>();
		// } else {
		attType = attToType[attName];
		// }
		
		// TODO: figure out how we get the actual att type instead of hard coding string
		pair<string, MyDB_AttTypePtr> attribute = make_pair(attName, attType);
		cout << "about to add in attribute " << attribute.first << " of type " <<  attribute.second->toString() <<"\n" << flush ;
		totSchema->appendAtt(attribute);
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
		MyDB_StatsPtr outputStats = make_shared<MyDB_Stats>(table)->costSelection(allDisjunctions);
		res = make_shared<LogicalTableScan>(
			table, 
			outputTable, 
			outputStats, 
			allDisjunctions);
		
		if (DEBUGBASECASE) {
			auto atts = table->getSchema()->getAtts();
			for (auto &[attName, _] : atts) {
				cout << "attribute of name " << attName  << "\n" << flush;
			}
		}
		// some code here...
		return make_pair (res, outputStats->getTupleCount());
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
