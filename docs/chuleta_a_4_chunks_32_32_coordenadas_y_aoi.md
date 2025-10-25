# Chuleta A4 — Chunks 32×32, coordenadas y AOI

> Mundo en tiles (1 unidad = 1 tile). Agrupamos en **chunks de 32×32** para organización y rendimiento.

---

## 1) Resumen rápido (qué recordar)
- **Tamaño de chunk**: `CHUNK_W = CHUNK_H = 32`
- **Posición → Chunk**: `cx = floor(x/32)`, `cy = floor(y/32)`  
  (en Java usa `Math.floorDiv(x, 32)` y `Math.floorDiv(y, 32)`)
- **Rango de tiles del chunk (cx,cy)**:  
  `x ∈ [cx·32, (cx+1)·32 − 1]`,  `y ∈ [cy·32, (cy+1)·32 − 1]`
- **Local dentro del chunk**: `lx = x − cx·32`, `ly = y − cy·32`  
  (siempre **0..31**)
- **AOI** con radio `r` (Chebyshev):  
  `AOI(cx,cy,r) = {(cx+dx, cy+dy) | dx,dy ∈ [−r..r]}` → **(2r+1)²** zonas.  
  Con `r=1` ⇒ **3×3 = 9** zonas.

---

## 2) Por qué 32
- **Potencia de 2 (2⁵)** → divisiones y límites limpios (…,-64,-32,0,32,64,…).
- **Equilibrio**: 32×32 = 1024 tiles por zona → ni demasiadas zonas, ni broadcasts enormes.
- **Menos churn**: cambias de zona cada 32 tiles.

---

## 3) Diagrama 1D (eje X)
```
... |<---- chunk -2 ---->|<---- chunk -1 ---->|<---- chunk 0 ---->|<---- chunk 1 ---->| ...
    -64                -33 -32              -1  0               31  32              63  64
                       [ -32 ..  -1 ]         [  0 ..  31 ]        [ 32 ..  63 ]
```
- Ejemplos:
  - `x = -33` → `cx = floor(-33/32) = -2`  (en rango [-64..-33])
  - `x = -1`  → `cx = -1`  (en rango [-32..-1])
  - `x = 0`   → `cx = 0`   (en rango [0..31])
  - `x = 31`  → `cx = 0`
  - `x = 32`  → `cx = 1`

---

## 4) Diagrama 2D (chunk y coordenada local)
```
Chunk (cx, cy)
┌────────────────────────────────┐
│  (lx,ly)=(0,31)  …         ↑ y │
│       ⋮                      │   │
│   tiles 32×32                │   │
│       ⋮                      │   │
│  (lx,ly)=(0,0)  … (31,0)    └──→ x
└────────────────────────────────┘
   lx,ly siempre en [0..31]
```
- Cálculo local: `lx = x − cx·32`, `ly = y − cy·32`.
- Casos límite útiles:
  - `x = -1, cx = -1` → `lx = -1 − (-1·32) = 31`
  - `x = -32, cx = -1` → `lx = 0`

---

## 5) Ejemplos trabajados (incluye negativos)
**Ejemplo A**: `x=65, y=-2`
- `cx = floor(65/32) = 2`, `cy = floor(-2/32) = -1`
- Rango X del chunk: `[2·32 .. 3·32−1] = [64 .. 95]`
- Local: `lx = 65 − 64 = 1`, `ly = -2 − (-32) = 30`
- **zoneKey**: `"2,-1"`

**Ejemplo B**: `x=-33, y=0`
- `cx = floor(-33/32) = -2`, `cy = floor(0/32) = 0`
- Local: `lx = -33 − (-2·32) = 31`, `ly = 0 − 0 = 0`

**Ejemplo C**: fronteras
- `x=31 → cx=0, lx=31`  |  `x=32 → cx=1, lx=0`
- `x=-1 → cx=-1, lx=31` |  `x=0  → cx=0,  lx=0`

---

## 6) AOI 3×3 con r=1
- Zonas suscritas: todas las combinaciones `dx,dy ∈ {−1,0,1}`.
- Al cambiar de chunk, **entra/sale** un borde de 3 zonas → coste bajo (delta de suscripciones).

```
   (cx-1,cy+1)  (cx,cy+1)  (cx+1,cy+1)
   (cx-1,cy)    (cx,cy)    (cx+1,cy)
   (cx-1,cy-1)  (cx,cy-1)  (cx+1,cy-1)
```

---

## 7) Errores comunes y cómo evitarlos
- **Truncar hacia 0** en lugar de **floor**: con negativos da chunks erróneos.  
  *Solución*: usa `floor`/`Math.floorDiv`.
- **Off-by-one** en rangos: recuerda el `−1` final para incluir el último tile.
- **Locales fuera de 0..31**: si `lx` o `ly` no está en ese rango, revisa `cx,cy`.

---

## 8) Glosario breve
- **Tile**: unidad básica (1×1).
- **Chunk (cx,cy)**: bloque de 32×32 tiles.
- **Local (lx,ly)**: posición interna al chunk (0..31).
- **AOI**: área de interés en coordenadas de chunks.
- **zoneKey**: cadena `"cx,cy"` para identificar la zona (opcional prefijo `worldId:`).

---

### Nota de impresión
- Orientación **Vertical (A4)**, márgenes **normales**. Imprimir al **100%** (sin “ajustar al área imprimible”).

