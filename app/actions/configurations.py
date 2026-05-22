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
    subject_type: str = FieldWithUIOptions(
        "vehicle",
        title="Subject Type",
        description=(
            "EarthRanger subject type applied to all observations from this integration. "
            "Common values: vehicle, person, animal."
        ),
    )
    emit_events: bool = FieldWithUIOptions(
        False,
        title="Forward Tracpoint events to Gundi",
        description=(
            "When enabled, Tracpoint position records tagged with an event "
            "(speeding, geofence breach, panic alert, etc.) are forwarded to "
            "Gundi as discrete events in addition to observations. "
            "Keep disabled until Gundi's dispatcher-side reference-data "
            "provisioning is deployed — otherwise EarthRanger will reject "
            "unknown event types on POST and event delivery will fail."
        ),
    )
    ui_global_options = GlobalUISchemaOptions(order=["subject_type", "emit_events"])
