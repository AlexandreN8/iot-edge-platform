def is_targeted(site_id, wave):
    """ True if this site is included in the update's target wave. """
    return site_id in wave


def format_status(site_id, sha, outcome, detail=""):
    """ Builds the payload published to ota_status after an update attempt. """
    return {
        "site_id": site_id,
        "sha": sha,
        "outcome": outcome,  # "success", "rolled_back", "failed"
        "detail": detail,
    }