import typing

TEST_XML_FILE = (
    "/data/p11-mw-base/frwiki-20251201-pages-articles-multistream1.xml-p1p306134"
)
MWM_BASE = "https://mirror.accum.se/mirror/wikimedia.org/dumps"
MW_FR_WIKI = "frwiki"
MW_FR_DATE = "20251201"
MW_EN_WIKI = "enwiki"
MW_EN_DATE = "20251201"
# https://mirror.accum.se/mirror/wikimedia.org/dumps/enwiki/20251201/enwiki-20251201-sha1sums.json
# "enwiki-20251201-pages-articles-multistream27.xml-p77475910p78975909.bz2"


MW_PAM_CATEGORY = "mw-page-article"
MW_PAM_PACK_URLS_QUEUE = "MW_PAM_PACK_URLS_QUEUE"
MW_PAM_RESULTS_QUEUE = "MW_PAM_RESULTS_QUEUE"

PAM_INDEX_CACHE_CAT = "pam_index"
PAM_PACK_CACHE_CAT = "pam_pack"


class PamError(Exception):
    pass


class PamPackStats(typing.TypedDict):
    pack_ref: str
    actual_count: int
    total_size: int
    total_zsize: int
    redirect_count: int
    other_count: int


def new_PamPackStats(pack_ref: str) -> PamPackStats:
    return PamPackStats(zip(PamPackStats.__annotations__, [pack_ref, 0, 0, 0, 0, 0]))  # type: ignore


def pam_base_for(
    wiki: str = MW_FR_WIKI, date: str = MW_FR_DATE, mwm_base: str = MWM_BASE
):
    return f"{mwm_base}/{wiki}/{date}"


def get_bool(value: str) -> bool:
    return value.lower() not in ("", "0", "false", "none") if value else False
