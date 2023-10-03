from typing import TypedDict


class ElasticSearchEntity(TypedDict):
    """Represents an entity in ElasticSearch."""

    dqscore_accuracy: str
    dqscore_completeness: str
    dqscore_timeliness: str
    dqscore_uniqueness: str
    dqscore_validity: str
    dqscorecnt_accuracy: str
    dqscorecnt_completeness: str
    dqscorecnt_timeliness: str
    dqscorecnt_uniqueness: str
    dqscorecnt_validity: str
    definition: str
    deriveddataentity: str
    deriveddatadomain: str
    deriveddatadomainguid: str
    deriveddataset: list[str]
    deriveddatasetguid: list[str]
    derivedperson: list[str]
    derivedpersonguid: list[str]
    guid: str
    id: str
    m4isourcetype: str
    name: str
    qualityguid_accuracy: str
    qualityguid_completeness: str
    qualityguid_timeliness: str
    qualityguid_uniqueness: str
    qualityguid_validity: str
    referenceablequalifiedname: str
    sourcetype: str
    supertypenames: list[str]
    typename: str
