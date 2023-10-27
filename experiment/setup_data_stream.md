PUT _ilm/policy/foo-lifecycle-policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_primary_shard_size": "50gb"
          }
        }
      },
      "warm": {
        "min_age": "30d",
        "actions": {
          "shrink": {
            "number_of_shards": 1
          },
          "forcemerge": {
            "max_num_segments": 1
          }
        }
      },
      "delete": {
        "min_age": "735d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}

# Create component template
# Creates a component template for mappings
PUT _component_template/foo-mappings
{
  "template": {
    "mappings": {
      "properties": {
        "@timestamp": {
          "type": "date",
          "format": "date_optional_time||epoch_millis"
        },
        "id": { "type": "keyword" },
        "name": { "type": "keyword" }
      }
    }
  },
  "_meta": {
    "description": "Mappings for @timestamp and id and name fields",
    "foo-custom-meta-field": "More arbitrary metadata"
  }
}

# Creates a component template for index settings
PUT _component_template/foo-settings
{
  "template": {
    "settings": {
      "index.lifecycle.name": "foo-lifecycle-policy"
    }
  },
  "_meta": {
    "description": "Settings for ILM",
    "foo-custom-meta-field": "More arbitrary metadata"
  }
}

PUT _index_template/foo-index-template
{
  "index_patterns": ["foo*"],
  "composed_of": [ "foo-mappings", "foo-settings" ],
  "priority": 500,
  "_meta": {
    "description": "Template for my time series data",
    "foo-custom-meta-field": "More arbitrary metadata"
  }
}
