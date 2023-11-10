engines = [
    {
        'name': 'atlas-dev',
        'schema': {'derivedfield': 'text', 'deriveddataattribute': 'text', 'deriveddataentity': 'text',
                   'dqscoresum_timeliness': 'number',
                   'qualityguid_completeness': 'text', 'sourcetype': 'text', 'deriveddataentityguid': 'text',
                   'dqscore_accuracy': 'number', 'dqscore_timeliness': 'number', 'dqscoresum_accuracy': 'number',
                   'dqscore_validity': 'number', 'derivedsystem': 'text', 'qualityguid_timeliness': 'text',
                   'deriveddomainleadguid': 'text', 'deriveddataset': 'text', 'derivedsystemguid': 'text',
                   'dqscoresum_completeness': 'number', 'dqscore_uniqueness': 'number',
                   'dqscorecnt_completeness': 'number',
                   'dqscoresum_uniqueness': 'number', 'breadcrumbname': 'text', 'breadcrumbguid': 'text',
                   'name': 'text',
                   'dqscore_overall': 'number', 'guid': 'text', 'dqscore_completeness': 'number',
                   'deriveddataattributeguid': 'text',
                   'dqscorecnt_uniqueness': 'number', 'deriveddatasetnames': 'text',
                   'referenceablequalifiedname': 'text',
                   'parentguid': 'text', 'derivedperson': 'text', 'dqscoresum_validity': 'number',
                   'dqscorecnt_overall': 'number',
                   'dqscorecnt_timeliness': 'number', 'entityname': 'text', 'derivedfieldguid': 'text',
                   'm4isourcetype': 'text',
                   'derivedentityguids': 'text', 'deriveddatasetguids': 'text', 'deriveddatasetguid': 'text',
                   'deriveddatastewardguid': 'text', 'supertypenames': 'text', 'classificationstext': 'text',
                   'qualityguid_uniqueness': 'text', 'deriveddataownerguid': 'text', 'definition': 'text',
                   'derivedpersonguid': 'text',
                   'dqscorecnt_validity': 'number', 'qualityguid_accuracy': 'text', 'email': 'text',
                   'derivedcollection': 'text',
                   'dqscorecnt_accuracy': 'number', 'deriveddatadomainguid': 'text', 'businessruleid': 'number',
                   'derivedcollectionguid': 'text', 'qualityguid_validity': 'text', 'dqscoresum_overall': 'number',
                   'deriveddatadomain': 'text', 'derivedentitynames': 'text', 'breadcrumbtype': 'text',
                   'typename': 'text',
                   'typealias': 'text'},
        'search-settings': {'search_fields': {'deriveddataentity': {'weight': 1}, 'derivedsystem': {'weight': 1},
                                              'deriveddataset': {'weight': 1}, 'name': {'weight': 1},
                                              'guid': {'weight': 1}, 'definition': {'weight': 1},
                                              'email': {'weight': 1}, 'derivedcollection': {'weight': 1},
                                              'deriveddatadomain': {'weight': 1}, 'typename': {'weight': 1},
                                              'id': {'weight': 1},'typealias': {'weight': 1}},
                            'result_fields': {'deriveddataentity': {'raw': {}}, 'derivedsystem': {'raw': {}},
                                              'deriveddataset': {'raw': {}},
                                              'name': {'snippet': {'size': 150, 'fallback': True}, 'raw': {}},
                                              'guid': {'raw': {}}, 'derivedperson': {'raw': {}},
                                              'supertypenames': {'raw': {}},
                                              'typealias': {'raw': {}},
                                              'definition': {'snippet': {'size': 200, 'fallback': True}, 'raw': {}},
                                              'derivedcollection': {'raw': {}}, 'deriveddatadomain': {'raw': {}},
                                              'typename': {'raw': {}}}, 'boosts': {}, 'precision': 2}
    },
    {
        'name': 'atlas-dev-quality',
        'schema': {'qualityguid': 'text', 'expression': 'text', 'dqscore': 'number',
                   'businessruleid': 'number', 'qualifiedname': 'text', 'qualityqualifiedname': 'text',
                   'datadomainname': 'text', 'fieldqualifiedname': 'text', 'fieldguid': 'text',
                   'dataqualityruledescription': 'text', 'dataqualityruledimension': 'text', 'name': 'text',
                   'guid': 'text'},
        'search-settings': {'search_fields': {'expression': {'weight': 1}, 'qualifiedname': {'weight': 1},
                                              'qualityqualifiedname': {'weight': 1},
                                              'datadomainname': {'weight': 1},
                                              'fieldqualifiedname': {'weight': 1},
                                              'dataqualityruledescription': {'weight': 3.1},
                                              'dataqualityruledimension': {'weight': 1}, 'id': {'weight': 1}},
                            'result_fields': {'qualityguid': {'raw': {}}, 'expression': {'raw': {}},
                                              'dqscore': {'raw': {}}, 'businessruleid': {'raw': {}},
                                              'qualifiedname': {'raw': {}}, 'id': {'raw': {}},
                                              'qualityqualifiedname': {'raw': {}},
                                              'datadomainname': {'raw': {}}, 'fieldqualifiedname': {'raw': {}},
                                              'fieldguid': {'raw': {}},
                                              'dataqualityruledescription': {'raw': {}},
                                              'dataqualityruledimension': {'raw': {}}, 'name': {'raw': {}},
                                              'guid': {'raw': {}}}, 'boosts': {}, 'precision': 2}
    },
    {
        'name': 'atlas-dev-gov-quality',
        'schema': {'expression': 'text', 'usedattributes': 'text', 'qualifiedname': 'text',
                   'qualityqualifiedname': 'text', 'dataqualityruletypename': 'text', 'result': 'text',
                   'dataqualitytype': 'text', 'dataqualityruledescription': 'text',
                   'dataqualityruledimension': 'text', 'doc_guid': 'text', 'result_id': 'text',
                   'business_rule_id': 'text', 'name': 'text', 'compliant': 'text', 'guid': 'text',
                   'entity_guid': 'text'},
        'search-settings': {
            'search_fields': {'expression': {'weight': 1}, 'usedattributes': {'weight': 1},
                              'qualifiedname': {'weight': 1},
                              'qualityqualifiedname': {'weight': 1}, 'dataqualityruletypename': {'weight': 1},
                              'result': {'weight': 1}, 'dataqualitytype': {'weight': 1},
                              'dataqualityruledescription': {'weight': 1}, 'dataqualityruledimension': {'weight': 1},
                              'doc_guid': {'weight': 1}, 'result_id': {'weight': 1}, 'business_rule_id': {'weight': 1},
                              'name': {'weight': 1}, 'compliant': {'weight': 1}, 'guid': {'weight': 1},
                              'entity_guid': {'weight': 1}, 'id': {'weight': 1}},
            'result_fields': {'expression': {'raw': {}}, 'usedattributes': {'raw': {}}, 'qualifiedname': {'raw': {}},
                              'id': {'raw': {}}, 'qualityqualifiedname': {'raw': {}},
                              'dataqualityruletypename': {'raw': {}},
                              'result': {'raw': {}}, 'dataqualitytype': {'raw': {}},
                              'dataqualityruledescription': {'raw': {}},
                              'dataqualityruledimension': {'raw': {}}, 'doc_guid': {'raw': {}},
                              'result_id': {'raw': {}},
                              'business_rule_id': {'raw': {}}, 'name': {'raw': {}}, 'compliant': {'raw': {}},
                              'guid': {'raw': {}}, 'entity_guid': {'raw': {}}}, 'boosts': {}, 'precision': 2}
    }
]
