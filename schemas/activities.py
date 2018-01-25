activities = {"name": "activities",
              "type": "record",
              "fields": [{"name": "activity_date",
                          "type": ["null", "string"],
                          "default": "null"},
                         {"name": "activity_type_id",
                          "type": ["null", "int"],
                          "default": "null"},
                         {"name": "attributes",
                          "type": {
                              "type": "array",
                              "items": [
                                {"name": "attribute",
                                 "type": "record",
                                 "fields": [
                                    {"name": "name",
                                     "type": ["null", "string"],
                                     "default": "null"},
                                    {"name": "value",
                                     "type": ["null", "string"],
                                     "default": "null"}
                                 ]}]}
                          },
                         {"name": "campaign_id",
                          "type": ["null", "int"],
                          "default": "null"},
                         {"name": "id",
                          "type": ["null", "int"],
                          "default": "null"},
                         {"name": "lead_id",
                          "type": ["null", "int"],
                          "default": "null"},
                         {"name": "marketo_guid",
                          "type": ["null", "string"],
                          "default": "null"},
                         {"name": "primary_attribute_value",
                          "type": ["null", "string"],
                          "default": "null"},
                         {"name": "primary_attribute_value_id",
                          "type": ["null", "int"],
                          "default": "null"}
                         ]
              }
