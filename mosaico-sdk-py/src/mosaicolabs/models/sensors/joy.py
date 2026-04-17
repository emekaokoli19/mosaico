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
    Currently, list-based fields such as `axes` and `buttons` are not directly queryable
    via the `.Q` proxy.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Retrieve joystick data (list fields like axes and buttons
            # are not directly queryable via the `.Q` proxy)
            qresponse = client.query()

            # Inspect the response
            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    axes: MosaicoType.list_[MosaicoType.float32] = MosaicoField(
        description="The axes measurements from a joystick."
    )
    """
    The axes measurements from a joystick.

    Represents continuous joystick inputs such as analog stick positions. Values typically range
    between -1.0 and 1.0 depending on the device.

    ### Querying with the **.Q Proxy**
    List-based fields such as `axes` are currently not directly queryable via the `.Q` proxy.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Retrieve joystick data (axes are list-based and not directly queryable)
            qresponse = client.query()

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    buttons: MosaicoType.list_[MosaicoType.int32] = MosaicoField(
        description="The buttons measurements from a joystick."
    )
    """
    The buttons measurements from a joystick.

    Represents discrete button states where 1 indicates pressed and 0 indicates released.

    ### Querying with the **.Q Proxy**
    List-based fields such as `buttons` are currently not directly queryable via the `.Q` proxy.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Retrieve joystick data (button states are list-based and not directly queryable)
            qresponse = client.query()

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """
