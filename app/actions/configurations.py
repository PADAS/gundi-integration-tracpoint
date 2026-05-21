import pydantic

from app.actions.core import AuthActionConfiguration, PullActionConfiguration
from app.services.utils import FieldWithUIOptions, UIOptions, GlobalUISchemaOptions


class AuthenticateConfig(AuthActionConfiguration):
    wsdl_url: str = FieldWithUIOptions(
        "http://www.terramarnetworks.net/v7/index.php?wsdl",
        title="WSDL URL",
        description=(
            "URL of the Tracpoint SOAP service WSDL. "
            "The default points to the public Terramar Networks v7 endpoint."
        ),
    )
    company: str = FieldWithUIOptions(
        ...,
        title="Company",
        description="Company name for your Tracpoint account (passed as userCompany in all API calls).",
    )
    username: str = FieldWithUIOptions(
        ...,
        title="Username",
        description="Username for the Tracpoint service account.",
    )
    password: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        format="password",
        title="Password",
        description="Password for the Tracpoint service account.",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["wsdl_url", "company", "username", "password"])


class PullObservationsConfig(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=30,
        title="Lookback Days",
        description=(
            "On the first run, fetch observations this many days back. "
            "Subsequent runs use an incremental cursor."
        ),
        ui_options=UIOptions(widget="range"),
    )
    subject_type: str = FieldWithUIOptions(
        "vehicle",
        title="Subject Type",
        description=(
            "EarthRanger subject type applied to all observations from this integration. "
            "Common values: vehicle, person, animal."
        ),
    )
    ui_global_options = GlobalUISchemaOptions(order=["lookback_days", "subject_type"])


class PullEventsConfig(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=30,
        title="Lookback Days",
        description=(
            "On the first run, fetch events this many days back. "
            "Subsequent runs use an incremental cursor."
        ),
        ui_options=UIOptions(widget="range"),
    )
    # TODO: Add any Tracpoint-specific event pull settings here
    # (e.g. event type filters, severity filters, etc.)
    ui_global_options = GlobalUISchemaOptions(order=["lookback_days"])
