import asyncio
import json
import secrets

from dataclasses import dataclass

from fastapi import FastAPI
from fastapi import WebSocket
from fastapi import WebSocketDisconnect


app = FastAPI(
    title="Age of Stick Relay"
)


ROOM_ALPHABET = (
    "ABCDEFGHJKLMNPQRSTUVWXYZ"
    "23456789"
)

MAX_MESSAGE_SIZE = (
    300_000
)


@dataclass
class Room:

    host: WebSocket

    guest: WebSocket | None = None


rooms = {}

rooms_lock = (
    asyncio.Lock()
)


def generate_room_code():

    while True:

        code = "".join(
            secrets.choice(
                ROOM_ALPHABET
            )
            for _ in range(
                6
            )
        )


        if code not in rooms:

            return code


async def send_json_safe(
    websocket,
    payload
):

    try:

        await websocket.send_text(
            json.dumps(
                payload,
                separators=(
                    ",",
                    ":"
                )
            )
        )

        return True


    except Exception:

        return False


@app.get("/")
async def health():

    return {
        "service":
            "Age of Stick Relay",

        "status":
            "online",

        "rooms":
            len(
                rooms
            )
    }


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket
):

    await websocket.accept()


    room_code = None
    role = None


    try:

        while True:

            raw_message = (
                await websocket.receive_text()
            )


            if (
                len(
                    raw_message
                )
                > MAX_MESSAGE_SIZE
            ):

                await send_json_safe(
                    websocket,
                    {
                        "type":
                            "error",

                        "message":
                            "Message is too large."
                    }
                )

                continue


            try:

                message = json.loads(
                    raw_message
                )


            except json.JSONDecodeError:

                await send_json_safe(
                    websocket,
                    {
                        "type":
                            "error",

                        "message":
                            "Invalid JSON message."
                    }
                )

                continue


            if not isinstance(
                message,
                dict
            ):

                continue


            message_type = (
                message.get(
                    "type"
                )
            )


            # =================================================
            # CREATE ROOM
            # =================================================

            if (
                message_type
                == "create_room"
            ):

                if room_code is not None:

                    continue


                async with rooms_lock:

                    room_code = (
                        generate_room_code()
                    )

                    rooms[
                        room_code
                    ] = Room(
                        host=websocket
                    )


                role = "host"


                await send_json_safe(
                    websocket,
                    {
                        "type":
                            "room_created",

                        "room_code":
                            room_code
                    }
                )


            # =================================================
            # JOIN ROOM
            # =================================================

            elif (
                message_type
                == "join_room"
            ):

                if room_code is not None:

                    continue


                requested_code = str(
                    message.get(
                        "room_code",
                        ""
                    )
                ).strip().upper()


                async with rooms_lock:

                    room = rooms.get(
                        requested_code
                    )


                    if room is None:

                        room = None


                    elif (
                        room.guest
                        is not None
                    ):

                        room = None


                    else:

                        room.guest = (
                            websocket
                        )


                if room is None:

                    await send_json_safe(
                        websocket,
                        {
                            "type":
                                "error",

                            "message":
                                (
                                    "Room not found or "
                                    "already full."
                                )
                        }
                    )

                    continue


                room_code = (
                    requested_code
                )

                role = (
                    "guest"
                )


                await send_json_safe(
                    websocket,
                    {
                        "type":
                            "room_joined",

                        "room_code":
                            room_code
                    }
                )


                await send_json_safe(
                    room.host,
                    {
                        "type":
                            "peer_joined"
                    }
                )


            # =================================================
            # RELAY GAME DATA
            # =================================================

            elif (
                message_type
                == "relay"
            ):

                if room_code is None:

                    continue


                payload = (
                    message.get(
                        "payload"
                    )
                )


                if not isinstance(
                    payload,
                    dict
                ):

                    continue


                async with rooms_lock:

                    room = rooms.get(
                        room_code
                    )


                if room is None:

                    continue


                peer = (
                    room.guest
                    if role
                    == "host"
                    else room.host
                )


                if peer is None:

                    continue


                await send_json_safe(
                    peer,
                    {
                        "type":
                            "relay",

                        "payload":
                            payload
                    }
                )


            # =================================================
            # KEEPALIVE
            # =================================================

            elif (
                message_type
                == "ping"
            ):

                await send_json_safe(
                    websocket,
                    {
                        "type":
                            "pong"
                    }
                )


            # =================================================
            # LEAVE
            # =================================================

            elif (
                message_type
                == "leave"
            ):

                break


    except WebSocketDisconnect:

        pass


    finally:

        if room_code is None:

            return


        peer = None


        async with rooms_lock:

            room = rooms.get(
                room_code
            )


            if room is None:

                return


            if role == "host":

                peer = (
                    room.guest
                )

                rooms.pop(
                    room_code,
                    None
                )


            else:

                if (
                    room.guest
                    is websocket
                ):

                    room.guest = (
                        None
                    )

                    peer = (
                        room.host
                    )


        if peer is not None:

            await send_json_safe(
                peer,
                {
                    "type":
                        "peer_left",

                    "message":
                        "The other player left the match."
                }
            )
