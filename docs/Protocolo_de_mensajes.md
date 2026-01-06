# WebSocket Protocol
## 1) Envelope (formato común)  
Todos los mensajes viajan como un JSON con dos campos:  
- type (string): identifica el tipo de mensaje.
- payload (any JSON): contenido asociado al tipo. Puede ser un objeto {}, array [], string, number, boolean o null (aunque recomendación: usar siempre objeto JSON para estabilidad).

```
JSON (forma general)
{
"type": "JOIN",
"payload": {}
}
```

###Reglas del envelope
type es case-sensitive y debe coincidir exactamente con uno de los valores del enum:  
- JOIN
- INITIAL_STATE 
- SUBSCRIBED
- SNAPSHOT_ZONE
- ERROR
- PLAYER_MOVE
- PLAYER_MOVED
- PLAYER_LOADED
- DESPAWN_ENTITIES

### payload:  
Debe seguir el esquema específico del type.  
Si un cliente recibe un type que no conoce, debe ignorarlo (forward compatibility) y opcionalmente loguearlo.

## 2) Mensajes del protocolo

### Servidor → Cliente
#### INITIAL_STATE  
Enviado en el flujo de conexión, contiene el estado inicial del jugador.
```json
{
"type": "INITIAL_STATE",
"payload": {
    "playerId": 123,
    "playerName": "Hero",
    "x": 10.5,
    "y": 0.0,
    "direction": "SOUTH"
  }
}
```
#### SUBSCRIBED  
Confirmación de suscripción a zonas AOI.
Esto esta puesto mayormente para debug en versiones iniciales.
```json
{
"type": "SUBSCRIBED",
"payload": {
    "subscribedChunks": [
          {
          "cx": 0,
           "cy": 0,
           "zoneKey": "0:0"
           },
          {
          "cx": 0,
           "cy": 1,
           "zoneKey": "0:1"
           },
           ...
    ]
  }
}
```

#### SNAPSHOT_ZONE  
Snapshot de un Chunk y las entidades a que hay en ese chunk(de momento solo players).
```json
{
"type": "SNAPSHOT_ZONE",
"payload": {
    "zoneKey": "0:0",
    "players": [
        {
        "playerId": 123,
        "playerName": "Hero",
        "x": 10.5,
        "y": 0.0,
        "direction": "SOUTH"
        },
        ...
      ]
    }
}
```
#### PLAYER_MOVED  
Notificación de que un jugador se ha movido en una zona suscrita.
```json
{
"type": "PLAYER_MOVED",
"payload": {
    "playerId": 123,
    "x": 15.0,
    "y": 0.0,
    "direction": "EAST"
  }
}
``` 

#### DESPAWN_ENTITIES  
Notificación de que ciertas entidades han salido de la zona suscrita.
```json
{
"type": "DESPAWN_ENTITIES",
"payload": {
    "zoneKey": "0:0",
    "entityIds": [123, 456, 789]
  }
}
```
#### PLAYER_LOADED  
Notificación de que un jugador ha cargado en el AOI.
```json
{"type": "PLAYER_LOADED",
"payload": {
    "playerId": 123,
    "playerName": "Hero",
    "x": 10.5,
    "y": 0.0,
    "direction": "SOUTH"
  }
}
```

### Cliente → Servidor
#### PLAYER_MOVE  
Notificación de que el jugador se ha movido.
```json
{
"type": "PLAYER_MOVE",
"payload": {
    "x": 15.0,
    "y": 0.0,
    "direction": "EAST"
  }
}```

