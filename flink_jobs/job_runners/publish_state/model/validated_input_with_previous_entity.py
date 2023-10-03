from dataclasses import dataclass

from dataclasses_json import LetterCase, dataclass_json

from flink_jobs.elastic_client import ElasticSearchEntity

from .validated_input import ValidatedInput


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class ValidatedInputWithPreviousEntity(ValidatedInput):
    previous_version: ElasticSearchEntity
