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
- DESPAWN_ZONES

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
    "y": 0.0
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
        "y": 0.0

