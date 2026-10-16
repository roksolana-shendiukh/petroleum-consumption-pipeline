import logging
from typing import Any

from mlflow.genai.scorers import (
    RelevanceToQuery,
    RetrievalGroundedness,
    RetrievalRelevance,
    Safety,
    ScorerSamplingConfig,
    delete_scorer,
    list_scorers,
)

logger = logging.getLogger(__name__)

JUDGES = {
    "Safety": Safety,
    "RelevanceToQuery": RelevanceToQuery,
    "RetrievalGroundedness": RetrievalGroundedness,
    "RetrievalRelevance": RetrievalRelevance,
}


def register_scorers(config: dict[str, Any]) -> list[str]:
    existing = {scorer.name for scorer in list_scorers()}
    started = []
    for item in config["scorers"]:
        name = item["name"]
        if name in existing:
            delete_scorer(name=name)
            logger.info("Replaced existing scorer %s", name)
        judge = JUDGES[item["judge"]]().register(name=name)
        judge.start(
            sampling_config=ScorerSamplingConfig(
                sample_rate=item["sample_rate"],
                filter_string=item["filter_string"],
            )
        )
        logger.info("Started scorer %s (%s) with sample rate %s", name, item["judge"], item["sample_rate"])
        started.append(name)
    return started