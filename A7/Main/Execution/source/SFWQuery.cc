
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"

const bool DEBUGBASECASE = false;
const bool DEBUG_OUTER_FN = false;
typedef vector<pair<string, MyDB_TablePtr>> subset;

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
	for (auto &table : this->tablesToProcess) {
		string tableName = table.first;
		string aliasName = table.second;
		MyDB_TablePtr aliasedTable = allTables[tableName]->alias(aliasName);

		// go through each aliased attribute in this table
		for (auto &att:  aliasedTable->getSchema()->getAtts()) {
			attToType.insert({att.first , att.second});
		}
		filteredTables.insert({aliasName, aliasedTable});
	}

	for (auto &val : this->valuesToSelect) {
		string attName = val->getId();
		MyDB_AttTypePtr attType;

		// check if its an agg type
		if (val->isAvg() || val->isSum()) {
			continue;
		}
		attType = attToType[attName];
		
		pair<string, MyDB_AttTypePtr> attribute = make_pair(attName, attType);
		totSchema->appendAtt(attribute);
	}

	// here we call the recursive, exhaustive enum. algorithm
	return optimizeQueryPlan (filteredTables, totSchema, this->allDisjunctions);
}

// first pair is left table subset, second pair is right table subset
vector<pair<subset, subset>> generateSubsets(map <string, MyDB_TablePtr> &allTables) {
	vector<pair<subset, subset>> subsets;	
	vector<pair<string, MyDB_TablePtr>> tableList;

	for (auto &table : allTables) {
		tableList.push_back(table);
	}

	int n = allTables.size();
	for (int i = 0; i < (1 << n); i++ ) {
		subset leftSubset;
		subset rightSubset;

		for (int j = 0; j < n; j++) {
			if (i & (1 << j)) {
				// Check if the jth bit is set
				leftSubset.push_back(tableList[j]);
			} else {
				rightSubset.push_back(tableList[j]);
			}
		}

		int lSize = leftSubset.size();
		// skip if left table gets > n / 2 to avoid duplicates
		if (lSize> n / 2 || lSize == 0) {
			continue;
		}

		subsets.push_back({leftSubset, rightSubset});
	}

	return subsets;
}

MyDB_SchemaPtr getAtts(subset tables, MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &topCNF) {
	MyDB_SchemaPtr schema = make_shared<MyDB_Schema>();
	for (auto &table : tables) {
		string tblName = table.first;
		MyDB_TablePtr tblPtr = table.second;
		for (auto &att : tblPtr->getSchema()->getAtts()) {
			string attName = att.first;

			// Check if the att is in totSchema
			if (totSchema->getAttByName(attName).first != -1) {
				schema->appendAtt(att);
				continue;
			}

			// Check if the att is in topCNF
			for (auto &expr : topCNF) {
				if (expr->referencesAtt(tblName, attName)) {
					schema->appendAtt(att);
					break;
				}
			}
		}
	}

	return schema;
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (
	map <string, MyDB_TablePtr> &allTables, 
	MyDB_SchemaPtr totSchema, 
	vector <ExprTreePtr> &allDisjunctions) {
	double best = 9e99;
	LogicalOpPtr res = nullptr;

	// some code here...
	string outputTableName = "tempTable" + to_string(this->tableNum++);
	MyDB_TablePtr outputTable = make_shared<MyDB_Table>(outputTableName, outputTableName, totSchema);
	if (DEBUGBASECASE) {
		cout << "Made the output table " << outputTableName << endl << flush;
		cout << "allTables size: " << allTables.size () << " \n" << flush ;
	}

	// case where no joins
	if (allTables.size () == 1) {
		MyDB_TablePtr table;
		for (const auto &tbl: allTables) {
			string key = tbl.first;
			MyDB_TablePtr tablePtr = tbl.second;
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
			for (auto &att : atts) {
				string attName = att.first;
				cout << "attribute of name " << attName  << "\n" << flush;
			}
		}
		// some code here...
		return make_pair (res, outputStats->getTupleCount());
	}

	// loop through every break into a LHS and a RHS
	auto subsets = generateSubsets(allTables);
	for (auto &sub : subsets) {
		subset left = sub.first;
		subset right = sub.second;
		vector <ExprTreePtr> leftCNF, rightCNF, topCNF;
		// get the CNF expressions: left, right, and top
		for (auto &d : allDisjunctions) {
			bool referencesLeft = false;
			bool referencesRight = false;
			// see if it refers to left
			for (auto table: left) {
				if (d->referencesTable(table.first)) {
					referencesLeft = true;
					break;
				}
			}

			// see if it refers to right
			for (auto table: right) {
				if (d->referencesTable(table.first)) {
					referencesRight = true;
					break;
				}
			}

			// add disjunction to appropriate CNF
			if (referencesLeft && !referencesRight) {
				leftCNF.push_back(d);
			} else if (!referencesLeft && referencesRight) {
				rightCNF.push_back(d);
			} else {
				topCNF.push_back(d);
			}
		}

			
		// create a vector of the left and right tables
		map<string, MyDB_TablePtr> leftTableMap = map<string, MyDB_TablePtr>();
		map<string, MyDB_TablePtr> rightTableMap = map<string, MyDB_TablePtr>();
		for (auto &tbl : left) {
			leftTableMap.insert(tbl);
		}
		for (auto &tbl : right) {
			rightTableMap.insert(tbl);
		}

		// figure out what atts we need from the left and the right
		MyDB_SchemaPtr leftSchema = getAtts(left, totSchema, topCNF);
		MyDB_SchemaPtr rightSchema = getAtts(right, totSchema, topCNF);
		
		pair <LogicalOpPtr, double> leftRes = this->optimizeQueryPlan(leftTableMap, leftSchema, leftCNF);
		pair <LogicalOpPtr, double> rightRes = this->optimizeQueryPlan(rightTableMap, rightSchema, rightCNF);
		
		// Let myExp = project_A (leftExp Join_topCNF rightExp)
		MyDB_StatsPtr myStats = leftRes.first->getStats()->costJoin(topCNF, rightRes.first->getStats());
		LogicalOpPtr myJoin = make_shared<LogicalJoin>(leftRes.first, rightRes.first, outputTable, allDisjunctions, myStats);
		// note: getStats gets T, V for bestExp, using the statistics in leftStats U rightStats

		double curCost = myStats->getTupleCount() + leftRes.second + rightRes.second;
		// if the cost is better, update best
		if (curCost < best) {
			best = curCost;
			res = myJoin;
		}
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
