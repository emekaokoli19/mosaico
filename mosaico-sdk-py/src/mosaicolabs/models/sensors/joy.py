"""
Joy Ontology Module.

Represents joystick input state.
"""

from mosaicolabs.models import MosaicoField, MosaicoType

from ..serializable import Serializable


class Joy(Serializable):
    """
    Joystick input state data.

    This class represents the state of a joystick device, including its continuous axis
    measurements and discrete button states.

    Attributes:
        axes: Joystick axis values representing analog stick positions and other continuous inputs.
        buttons: Button states representing digital inputs (pressed or not pressed).

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter joystick data based
    on axis or button values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for joystick axis values within a range
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.axes.between(-1.0, 1.0))
            )

            # Inspect the response
            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for specific button states
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.buttons.eq(1), include_timestamp_range=True)
            )

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {item.topics}")
        ```
    """

    axes: list[MosaicoType.float32] = MosaicoField(
        description="The axes measurements from a joystick."
    )
    """
    The axes measurements from a joystick.

    Represents continuous joystick inputs such as analog stick positions. Values typically range
    between -1.0 and 1.0 depending on the device.

    ### Querying with the **.Q Proxy**
    The axes field is queryable via the `axes` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | Joy.Q.axes | Numeric (array elements) | .eq(), .neq(), .lt(), .gt(), .leq(), .geq(), .in_(), .between() |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for joystick axis values within a range
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.axes.between(-1.0, 1.0))
            )

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    buttons: list[MosaicoType.int32] = MosaicoField(
        description="The buttons measurements from a joystick."
    )
    """
    The buttons measurements from a joystick.

    Represents discrete button states where 1 indicates pressed and 0 indicates released.

    ### Querying with the **.Q Proxy**
    The buttons field is queryable via the `buttons` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | Joy.Q.buttons | Numeric (array elements) | .eq(), .neq(), .lt(), .gt(), .leq(), .geq(), .in_(), .between() |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for button press events
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.buttons.eq(1))
            )

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """
