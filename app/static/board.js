(() => {
  const body = document.body;
  const boardId = body.dataset.boardId;
  const pageToken = body.dataset.token || new URLSearchParams(location.search).get("token") || "";
  let currentWsToken = body.dataset.wsToken || pageToken;
  let currentWsTokenExp = Number(body.dataset.wsTokenExp || 0);
  let wsRefreshToken = body.dataset.wsRefreshToken || "";
  let wsRefreshTokenExp = Number(body.dataset.wsRefreshTokenExp || 0);
  let wsTokenRefreshPromise = null;
  const localUsername = body.dataset.username || "user";

  const boardWrap = document.getElementById("boardWrap");
  const canvasEl = document.getElementById("canvas");
  const cursorLayer = document.getElementById("cursorLayer");

  const pencilToolBtn = document.getElementById("pencilTool");
  const shapeToolBtn = document.getElementById("shapeTool");
  const textToolBtn = document.getElementById("textTool");
  const imageToolBtn = document.getElementById("imageTool");
  const stickerToolBtn = document.getElementById("stickerTool");
  const undoBtn = document.getElementById("undoBtn");
  const redoBtn = document.getElementById("redoBtn");
  const undoRedoDock = document.getElementById("undoRedoDock");
  const clearBtn = document.getElementById("clearBtn");
  const bgBtn = document.getElementById("bgBtn");
  const miroImportBtn = document.getElementById("miroImportBtn");
  const hiddenImageInput = document.getElementById("hiddenImageInput");
  const modalOverlay = document.getElementById("modalOverlay");
  const confirmModal = document.getElementById("confirmModal");
  const confirmModalTitle = document.getElementById("confirmModalTitle");
  const confirmModalBody = document.getElementById("confirmModalBody");
  const confirmModalOk = document.getElementById("confirmModalOk");
  const confirmModalCancel = document.getElementById("confirmModalCancel");
  const miroModal = document.getElementById("miroModal");
  const miroBoardUrlInput = document.getElementById("miroBoardUrlInput");
  const miroTokenInput = document.getElementById("miroApiValue");
  const miroRememberToken = document.getElementById("miroRememberToken");
  const miroModalStatus = document.getElementById("miroModalStatus");
  const miroModalOk = document.getElementById("miroModalOk");
  const miroModalOkLabel = document.getElementById("miroModalOkLabel");
  const miroModalCancel = document.getElementById("miroModalCancel");
  const boardNoticeEl = document.getElementById("boardNotice");

  const pencilPanel = document.getElementById("pencilPanel");
  const customColorPanel = document.getElementById("customColorPanel");
  const customColorSv = document.getElementById("customColorSv");
  const customColorSvCursor = document.getElementById("customColorSvCursor");
  const customColorHue = document.getElementById("customColorHue");
  const customColorHueCursor = document.getElementById("customColorHueCursor");
  const textPanel = document.getElementById("textPanel");
  const shapePanel = document.getElementById("shapePanel");
  const palette = document.getElementById("palette");

  const strokeWidthEl = document.getElementById("strokeWidth");
  const textSizeEl = document.getElementById("textSize");
  const cornerRadiusEl = document.getElementById("cornerRadius");
  const selectionPalette = document.getElementById("selectionPalette");
  const selectionFillPalette = document.getElementById("selectionFillPalette");
  const selectionStrokeWidthEl = document.getElementById("selectionStrokeWidth");
  const stickerEditOverlay = document.getElementById("stickerEditOverlay");

  // Mini contextual toolbar (floats above the selection) and its popovers.
  const selectionToolbar = document.getElementById("selectionToolbar");
  const selColorBtn = document.getElementById("selColorBtn");
  const selColorDot = document.getElementById("selColorDot");
  const selFillBtn = document.getElementById("selFillBtn");
  const selStrokeBtn = document.getElementById("selStrokeBtn");
  const selCornerBtn = document.getElementById("selCornerBtn");
  const selLockBtn = document.getElementById("selLockBtn");
  const selLockIcon = document.getElementById("selLockIcon");
  const selRotateBtn = document.getElementById("selRotateBtn");
  const selDeleteBtn = document.getElementById("selDeleteBtn");
  const selDeleteSep = document.getElementById("selDeleteSep");
  const selColorPopover = document.getElementById("selColorPopover");
  const selFillPopover = document.getElementById("selFillPopover");
  const selStrokePopover = document.getElementById("selStrokePopover");
  const selCornerPopover = document.getElementById("selCornerPopover");

  const zoomOutBtn = document.getElementById("zoomOutBtn");
  const zoomInBtn = document.getElementById("zoomInBtn");
  const zoomCenterBtn = document.getElementById("zoomCenterBtn");
  const miniMapEl = document.getElementById("miniMap");
  const miniCtx = miniMapEl.getContext("2d");
  const miniWrap = document.getElementById("miniWrap");
  const miniCollapseBtn = document.getElementById("miniCollapseBtn");

  if (!boardWrap || !canvasEl || !miniMapEl || !miniCtx) {
    console.error("Whiteboard init failed: required DOM nodes are missing.");
    return;
  }

  const paletteColors = ["#1f2937", "#2563eb", "#0f766e", "#7c3aed", "#be123c", "#ea580c", "#16a34a"];
  const STICKER_COLORS = ["#fef08a", "#fecaca", "#bbf7d0", "#bfdbfe", "#fde68a", "#e9d5ff"];
  const STICKER_DEFAULT_W = 200;
  const STICKER_DEFAULT_H = 160;
  const STICKER_MIN_SIZE = 100;
  const STICKER_DEFAULT_FONT_SIZE = 18;
  const STICKER_MIN_FONT_SIZE = 10;
  const STICKER_TEXT_PAD = 14;
  const STICKER_LINE_HEIGHT_RATIO = 1.3;
  const STICKER_FONT_FAMILY = "Montserrat, 'Segoe UI', sans-serif";

  // Wraps text the same way the CSS overlay does (word-wrap, breaking only a
  // single word too long to fit its own line) using real canvas text
  // measurement, so the final render matches what the user saw while typing.
  function wrapStickyNoteLines(ctx, text, fontPx, maxWidth) {
    ctx.font = `500 ${fontPx}px ${STICKER_FONT_FAMILY}`;
    const breakLongWord = (word) => {
      const chars = Array.from(word);
      const out = [];
      let current = "";
      chars.forEach((ch) => {
        const candidate = current + ch;
        if (current && ctx.measureText(candidate).width > maxWidth) {
          out.push(current);
          current = ch;
        } else {
          current = candidate;
        }
      });
      if (current) out.push(current);
      return out.length ? out : [""];
    };
    const lines = [];
    String(text == null ? "" : text).split("\n").forEach((para) => {
      if (!para) {
        lines.push("");
        return;
      }
      let current = "";
      para.split(" ").forEach((word) => {
        if (ctx.measureText(word).width > maxWidth) {
          if (current) {
            lines.push(current);
            current = "";
          }
          const broken = breakLongWord(word);
          broken.slice(0, -1).forEach((l) => lines.push(l));
          current = broken[broken.length - 1] || "";
          return;
        }
        const candidate = current ? `${current} ${word}` : word;
        if (current && ctx.measureText(candidate).width > maxWidth) {
          lines.push(current);
          current = word;
        } else {
          current = candidate;
        }
      });
      lines.push(current);
    });
    return lines;
  }

  // Single-object sticky note (replaces the old fabric.Group(Rect, Textbox)
  // pair). Extending Rect means resize/hit-testing/serialization are all
  // Rect's own well-tested behavior for free, and - critically - a real
  // single object can never be misidentified as a multi-selection by
  // isActiveSelectionObject()/getObjects()-based checks the way a Group
  // could, which is exactly what let a dragged/resized sticker's rect and
  // text leak onto the board as two independent synced objects. Text is
  // drawn fresh via ctx.fillText on every render (see _renderNoteText)
  // rather than living in a child object.
  class StickyNote extends fabric.Rect {
    static type = "StickyNote";

    static ownDefaults = {
      noteText: "",
      noteFontSize: STICKER_DEFAULT_FONT_SIZE,
      noteTextColor: "#1f2937",
      shapeKind: "sticker",
      rx: 12,
      ry: 12,
      stroke: "rgba(15,23,42,0.14)",
      strokeWidth: 1,
      lockUniScaling: true,
      lockScalingFlip: true,
    };

    static getDefaults() {
      return { ...super.getDefaults(), ...StickyNote.ownDefaults };
    }

    constructor(options) {
      super();
      Object.assign(this, StickyNote.ownDefaults);
      this.setOptions(options);
    }

    toObject(propertiesToInclude = []) {
      return super.toObject(["noteText", "noteFontSize", "noteTextColor", "shapeKind", ...propertiesToInclude]);
    }

    _render(ctx) {
      super._render(ctx);
      this._renderNoteText(ctx);
    }

    _renderNoteText(ctx) {
      const maxWidth = Math.max(1, this.width - STICKER_TEXT_PAD * 2);
      const fontPx = Number(this.noteFontSize) || STICKER_DEFAULT_FONT_SIZE;
      const lineHeight = fontPx * STICKER_LINE_HEIGHT_RATIO;
      const lines = wrapStickyNoteLines(ctx, this.noteText, fontPx, maxWidth);
      ctx.save();
      ctx.font = `500 ${fontPx}px ${STICKER_FONT_FAMILY}`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = this.noteTextColor || "#1f2937";
      const totalHeight = lines.length * lineHeight;
      let y = -this.height / 2 + Math.max(STICKER_TEXT_PAD, (this.height - totalHeight) / 2) + lineHeight / 2;
      lines.forEach((line) => {
        ctx.fillText(line, 0, y);
        y += lineHeight;
      });
      ctx.restore();
    }
  }
  fabric.classRegistry.setClass(StickyNote);

  const cursorColors = ["#e5484d", "#2563eb", "#0d9488", "#ea580c", "#0891b2", "#c026d3", "#b8790a", "#7c3aed"];
  const GRID_WORLD_SIZE = 24;
  // Dot grid (not lines) - reads as lighter/more premium at a glance, matches
  // the default background painted by CSS on #boardWrap before any JS runs.
  const GRID_BG_IMAGE = "radial-gradient(circle, rgba(13,58,33,0.15) 1.1px, transparent 1.6px)";
  const MAX_IMAGE_IMPORT_SIDE = 2400;
  const TARGET_IMAGE_BYTES = 2 * 1024 * 1024;
  const MAX_BOARD_BYTES = 30 * 1024 * 1024;
  const BOARD_SOFT_LIMIT_BYTES = MAX_BOARD_BYTES - 220 * 1024;
  const LOAD_FROM_JSON_TIMEOUT_MS = 8000;

  let currentTool = "select";
  let currentShapeType = "arrow";
  let currentStickerColor = STICKER_COLORS[0];
  let currentColor = paletteColors[0];
  let currentBackground = "grid";
  let canEdit = false;
  let boardRoleCanEdit = false;
  let allowStudentsDraw = true;
  let debugForceEdit = false;
  let canClear = false;
  let myJwtRole = "";
  let myClientId = "";
  let myUserId = "";
  let isSocketConnected = false;
  let isBoardInitialized = false;
  let lastConnectionNoticeAt = 0;
  let hadConnectionDrop = false;

  let isRemoteApplying = false;
  let suppressBroadcast = false;
  let pendingOpsTimer = null;
  let pendingOps = [];
  let remoteOpsChain = Promise.resolve();
  let cursorAnimFrame = 0;
  let pendingTextSyncTimer = null;
  let localOpSeq = 0;
  let lastSeenSeqId = 0;
  let boardNoticeTimer = null;
  let copiedSelectionPayload = [];
  const ACTIVE_SURFACE_ID = "main";
  const PASTE_SHIFT_STEP = 24;
  let pasteShiftCount = 0;
  const jsonEncoder = typeof TextEncoder !== "undefined" ? new TextEncoder() : null;

  let panMode = false;
  let panLast = { x: 0, y: 0 };
  let drawingShape = null;
  let drawingStart = null;
  let erasing = false;

  let pinchMode = false;
  let pinchDist = 0;
  let pinchCenter = null;
  let suppressTouchInputUntil = 0;
  let suppressPathCreatedUntil = 0;

  let skipNextTextCreate = false;
  let textEditArmedObjId = "";
  let focusedLockedObject = null;

  let lastCursorSentAt = 0;
  const remoteCursors = new Map();
  const supportsPointerEvents = typeof window !== "undefined" && "PointerEvent" in window;
  const activeTouchPointers = new Map();
  const customColorState = { h: 217, s: 0.44, v: 0.22 };
  let customColorDragMode = "";
  let stickerMeasureProbe = null;
  // The gradient SV/hue picker panel is a single shared DOM instance used
  // from two places: the toolbar's pencil-color trigger (mode "pencil",
  // changes the global drawing color) and the mini selection-toolbar's
  // color trigger (mode "selection", recolors the current selection without
  // touching the pencil color). This flag decides which one a drag updates.
  let customColorMode = "pencil";

  let miniState = {
    minX: 0,
    minY: 0,
    scale: 1,
    ox: 0,
    oy: 0,
    vpRect: { x: 0, y: 0, w: 0, h: 0 },
  };
  let miniDragging = false;
  const historyIdStorageKey = `whiteboard-history-id:${boardId}`;
  const historyClientId = (() => {
    try {
      const existing = sessionStorage.getItem(historyIdStorageKey);
      if (existing) return existing;
      const created = (crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`).replace(/\s+/g, "");
      sessionStorage.setItem(historyIdStorageKey, created);
      return created;
    } catch (_) {
      return (crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`).replace(/\s+/g, "");
    }
  })();

  function parseJwtExp(tokenValue) {
    if (!tokenValue) return 0;
    const chunks = String(tokenValue).split(".");
    if (chunks.length < 2) return 0;
    try {
      const normalized = chunks[1].replace(/-/g, "+").replace(/_/g, "/");
      const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
      const payload = JSON.parse(atob(padded));
      const exp = Number(payload?.exp || 0);
      return Number.isFinite(exp) ? exp : 0;
    } catch (_) {
      return 0;
    }
  }

  function effectiveTokenExp(tokenValue, fallbackExp) {
    const parsed = parseJwtExp(tokenValue);
    if (parsed > 0) return parsed;
    const fromFallback = Number(fallbackExp || 0);
    return Number.isFinite(fromFallback) ? fromFallback : 0;
  }

  function tokenExpiresSoon(expEpochSeconds, skewSeconds = 60) {
    const exp = Number(expEpochSeconds || 0);
    if (!Number.isFinite(exp) || exp <= 0) return false;
    const now = Math.floor(Date.now() / 1000);
    return exp - now <= skewSeconds;
  }

  function isAuthConnectError(err) {
    const message = String(err?.message || "");
    return /token expired|invalid token|missing token|token missing/i.test(message);
  }

  async function refreshWsToken(force = false) {
    if (!wsRefreshToken) return false;
    if (!force && !tokenExpiresSoon(currentWsTokenExp, 75)) return true;
    if (wsTokenRefreshPromise) return wsTokenRefreshPromise;

    wsTokenRefreshPromise = (async () => {
      try {
        const res = await fetch("/api/ws-token/refresh", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            board_id: boardId,
            refresh_token: wsRefreshToken,
          }),
        });
        if (!res.ok) return false;
        const payload = await res.json();
        const nextWsToken = String(payload?.ws_token || "");
        const nextRefreshToken = String(payload?.ws_refresh_token || "");
        if (!nextWsToken || !nextRefreshToken) return false;
        currentWsToken = nextWsToken;
        currentWsTokenExp = effectiveTokenExp(nextWsToken, payload?.ws_token_exp);
        wsRefreshToken = nextRefreshToken;
        wsRefreshTokenExp = effectiveTokenExp(nextRefreshToken, payload?.ws_refresh_token_exp);
        return true;
      } catch (_) {
        return false;
      }
    })().finally(() => {
      wsTokenRefreshPromise = null;
    });

    return wsTokenRefreshPromise;
  }

  currentWsTokenExp = effectiveTokenExp(currentWsToken, currentWsTokenExp);
  wsRefreshTokenExp = effectiveTokenExp(wsRefreshToken, wsRefreshTokenExp);

  const socket = io({
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 500,
    reconnectionDelayMax: 4000,
    upgrade: true,
    rememberUpgrade: false,
    timeout: 20000,
    auth: (cb) => {
      if (!currentWsToken) {
        cb({ token: currentWsToken, board_id: boardId, history_id: historyClientId });
        return;
      }
      refreshWsToken(false)
        .catch(() => false)
        .finally(() => cb({ token: currentWsToken, board_id: boardId, history_id: historyClientId }));
    },
  });

  const fabricCanvas = new fabric.Canvas(canvasEl, {
    selection: true,
    preserveObjectStacking: true,
    fireMiddleClick: true,
    targetFindTolerance: 10,
  });
  fabricCanvas.uniformScaling = false;

  function setupObjectStyles() {
    fabricCanvas.selectionColor = "rgba(22,163,74,0.08)";
    fabricCanvas.selectionBorderColor = "#16a34a";
    fabricCanvas.selectionLineWidth = 1.5;

    fabric.Object.prototype.set({
      borderColor: "#16a34a",
      borderScaleFactor: 1.5,
      borderDashArray: null,
      cornerColor: "#ffffff",
      cornerStrokeColor: "#16a34a",
      transparentCorners: false,
      cornerStyle: "rect",
      cornerSize: 10,
      touchCornerSize: 28,
      borderOpacityWhenMoving: 0.95,
      lockRotation: true,
      lockUniScaling: false,
      padding: 8,
    });
  }

  function setCornerOnlyControls(target) {
    if (!target || typeof target.setControlsVisibility !== "function") return;
    target.setControlsVisibility({
      tl: true,
      tr: true,
      bl: true,
      br: true,
      mt: false,
      mb: false,
      ml: false,
      mr: false,
      mtr: false,
    });
  }

  // Rotation now lives only on the mini selection toolbar (selRotateBtn),
  // whose visibility already tracks the active selection via
  // updateSelectionToolbar() - kept as a thin alias so the many existing
  // call sites (selection change, tool switch, etc.) don't need touching.
  function updateRotateButtonState() {
    updateSelectionToolbar();
  }

  function isObjectLocked(obj) {
    return !!(obj && obj.wb_locked);
  }

  function setObjectLocked(obj, locked) {
    if (!obj || typeof obj.set !== "function") return;
    const next = !!locked;
    obj.wb_locked = next;
    obj.set({
      lockMovementX: next,
      lockMovementY: next,
      lockScalingX: next,
      lockScalingY: next,
      lockSkewingX: next,
      lockSkewingY: next,
      lockRotation: true,
      editable: !next,
      hoverCursor: next ? "not-allowed" : "move",
    });
    if (next) {
      obj.selectable = false;
      obj.hasControls = false;
      obj.hasBorders = false;
    } else {
      obj.hasControls = true;
      obj.hasBorders = true;
    }
    obj.setCoords();
  }

  function syncLockStates() {
    fabricCanvas.forEachObject((obj) => setObjectLocked(obj, isObjectLocked(obj)));
  }

  function isObjectOnCanvas(obj) {
    return !!(obj && obj.canvas === fabricCanvas);
  }

  function isLockEligibleObject(obj) {
    return !!(obj && String(obj.type || "").toLowerCase() === "image");
  }

  function isTextObject(obj) {
    const t = String(obj && obj.type || "").toLowerCase();
    return t === "i-text" || t === "text" || t === "textbox";
  }

  function isTextModeSelectableObject(obj) {
    return !!obj && isTextObject(obj);
  }

  function isNoMirrorObject(obj) {
    if (!obj) return false;
    // Stickers draw their own text via ctx.fillText inside _render (see the
    // StickyNote class) rather than a separate child object - if the note
    // itself ever ended up flipped, the text would render mirrored right
    // along with it, so it needs the same flip-prevention text/images get.
    return isTextObject(obj) || isStickerObject(obj) || String(obj.type || "").toLowerCase() === "image";
  }

  function isUniformScaleObject(obj) {
    // isNoMirrorObject already covers stickers (see above) - a sticker's
    // scaleX/scaleY must stay locked together so its text (drawn fresh via
    // ctx.fillText on every render, using the note's current width/height)
    // doesn't get stretched by a non-uniform canvas transform.
    // applySelectionStyles() re-derives lockUniScaling from this check every
    // time an object is (re)selected, which previously silently overrode the
    // lockUniScaling:true set at sticker creation time.
    return isNoMirrorObject(obj);
  }

  function enforceNoMirrorObject(obj) {
    if (!obj || !isNoMirrorObject(obj) || typeof obj.set !== "function") return false;
    const nextScaleX = Math.max(0.001, Math.abs(Number(obj.scaleX || 1)));
    const nextScaleY = Math.max(0.001, Math.abs(Number(obj.scaleY || 1)));
    const enforceUniformScale = isUniformScaleObject(obj);
    const nextScale = enforceUniformScale ? Math.max(nextScaleX, nextScaleY) : 0;
    const mustDisableFlip = !!obj.flipX || !!obj.flipY;
    const mustLockFlipScale = obj.lockScalingFlip !== true;
    const mustLockUniScale = enforceUniformScale && obj.lockUniScaling !== true;
    const changed =
      !Number.isFinite(Number(obj.scaleX)) || !Number.isFinite(Number(obj.scaleY))
      || nextScaleX !== Number(obj.scaleX || 1)
      || nextScaleY !== Number(obj.scaleY || 1)
      || (enforceUniformScale && (nextScaleX !== nextScale || nextScaleY !== nextScale))
      || mustDisableFlip
      || mustLockFlipScale
      || mustLockUniScale;
    if (!changed) return false;
    const nextProps = {
      flipX: false,
      flipY: false,
      lockScalingFlip: true,
    };
    if (enforceUniformScale) {
      nextProps.scaleX = nextScale;
      nextProps.scaleY = nextScale;
      nextProps.lockUniScaling = true;
    } else {
      nextProps.scaleX = nextScaleX;
      nextProps.scaleY = nextScaleY;
    }
    obj.set(nextProps);
    obj.setCoords();
    return true;
  }

  function enforceNoMirrorInTarget(target) {
    if (!target) return false;
    if (isMultiSelectionObject(target) && typeof target.getObjects === "function") {
      let changedAny = false;
      target.getObjects().forEach((obj) => {
        if (enforceNoMirrorObject(obj)) changedAny = true;
      });
      return changedAny;
    }
    return enforceNoMirrorObject(target);
  }

  // Any current selection (single object, a grouped object like an arrow or
  // sticker treated as one unit, a same-type multi-select, or - a locked
  // object the user just clicked, which Fabric won't actually "select" since
  // locked objects have selectable:false but the app still wants to offer an
  // unlock affordance for it).
  function resolveSelectionAnchor() {
    const active = fabricCanvas.getActiveObject();
    if (active) {
      return { objects: getPanelSelectionObjects(), anchor: active };
    }
    if (focusedLockedObject && isObjectOnCanvas(focusedLockedObject) && isObjectLocked(focusedLockedObject)) {
      return { objects: [focusedLockedObject], anchor: focusedLockedObject };
    }
    return { objects: [], anchor: null };
  }

  // Positions `el` centered above `anchor`'s bounding box in page coordinates,
  // flipping to below the object if there isn't room above, and clamping to
  // stay fully on-screen. Shared by the selection mini-toolbar; same math the
  // old standalone lock button used.
  function placeAboveAnchor(el, anchor, gapPx = 10) {
    if (!el || !anchor || typeof anchor.getBoundingRect !== "function") return false;
    if (typeof anchor.isOnScreen === "function" && !anchor.isOnScreen()) return false;
    if (typeof anchor.setCoords === "function") anchor.setCoords();
    const bounds = anchor.getBoundingRect();
    if (!bounds) return false;
    const bw = Number(bounds.width || 0);
    const bh = Number(bounds.height || 0);
    // getBoundingRect() returns scene/world coordinates, not screen pixels -
    // comparing them directly against canvas width/height (as the original
    // lock-button code did) only happened to work when the viewport pan was
    // (0,0); with the default centered pan (resetView sets it to half the
    // canvas size) it rejected on-screen objects. Convert corners through the
    // viewport transform before checking.
    const screenTopLeft = screenFromWorld(bounds.left, bounds.top);
    const screenBottomRight = screenFromWorld(bounds.left + bw, bounds.top + bh);
    const intersectsCanvas =
      Math.max(screenTopLeft.x, screenBottomRight.x) >= 0
      && Math.max(screenTopLeft.y, screenBottomRight.y) >= 0
      && Math.min(screenTopLeft.x, screenBottomRight.x) <= fabricCanvas.getWidth()
      && Math.min(screenTopLeft.y, screenBottomRight.y) <= fabricCanvas.getHeight();
    if (!intersectsCanvas) return false;
    let worldX = bounds.left + bw / 2;
    let worldY = bounds.top;
    if (typeof anchor.getCoords === "function") {
      const coords = anchor.getCoords();
      if (Array.isArray(coords) && coords.length) {
        let minY = Number.POSITIVE_INFINITY;
        let minX = Number.POSITIVE_INFINITY;
        let maxX = Number.NEGATIVE_INFINITY;
        coords.forEach((pt) => {
          const px = Number(pt?.x);
          const py = Number(pt?.y);
          if (!Number.isFinite(px) || !Number.isFinite(py)) return;
          if (py < minY) minY = py;
          if (px < minX) minX = px;
          if (px > maxX) maxX = px;
        });
        if (Number.isFinite(minY) && Number.isFinite(minX) && Number.isFinite(maxX)) {
          worldX = (minX + maxX) / 2;
          worldY = minY;
        }
      }
    }
    const canvasRect = canvasEl.getBoundingClientRect();
    const topCenter = screenFromWorld(worldX, worldY);
    const absX = canvasRect.left + topCenter.x;
    const absTop = canvasRect.top + topCenter.y;
    const elW = el.offsetWidth || 160;
    const elH = el.offsetHeight || 36;

    let top = absTop - elH - gapPx;
    if (top < 8) {
      const bottomCenter = screenFromWorld(worldX, bounds.top + bh);
      top = canvasRect.top + bottomCenter.y + gapPx;
    }
    top = Math.max(8, Math.min(window.innerHeight - elH - 8, top));
    const left = Math.max(8, Math.min(window.innerWidth - elW - 8, absX - elW / 2));
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    return true;
  }

  function setSelBtnVisible(btn, visible) {
    if (btn) btn.disabled = !visible;
  }

  // Tracks whichever sel-popover is currently open and the button it's
  // anchored to, so repositionOpenSelPopovers() (called every time the
  // mini-toolbar itself moves - pan, zoom, selection change) can keep it
  // glued to that button instead of it staying put in screen space while
  // the object/toolbar it belongs to moves out from under it.
  let activeSelPopover = null;

  function closeSelPopovers() {
    [selColorPopover, selFillPopover, selStrokePopover, selCornerPopover].forEach((p) => p && p.classList.remove("active"));
    activeSelPopover = null;
    // The gradient color picker is a shared panel (see customColorMode) - if
    // it's currently showing the selection's color, closing the mini
    // toolbar's popovers should close it too instead of leaving it orphaned
    // above a selection that no longer has a toolbar under it.
    if (customColorPanel && (customColorMode === "selection" || customColorMode === "selectionFill")) {
      customColorPanel.classList.remove("active");
    }
  }

  function toggleSelPopover(popoverEl, triggerBtn) {
    if (!popoverEl || !triggerBtn) return;
    const isOpen = popoverEl.classList.contains("active");
    closeSelPopovers();
    if (!isOpen) {
      popoverEl.classList.add("active");
      placePanelNear(popoverEl, triggerBtn);
      activeSelPopover = { popover: popoverEl, trigger: triggerBtn };
    }
  }

  // Re-anchors whatever sel-popover (or the shared customColorPanel, when
  // it's the mini-toolbar's own color/fill trigger that opened it) is
  // currently open - called alongside placeAboveAnchor(selectionToolbar, ...)
  // so these follow the object exactly like the mini-toolbar itself does,
  // instead of staying fixed in screen space while the canvas pans under them.
  function repositionOpenSelPopovers() {
    if (activeSelPopover && activeSelPopover.popover.classList.contains("active")) {
      placePanelNear(activeSelPopover.popover, activeSelPopover.trigger);
    }
    if (customColorPanel && customColorPanel.classList.contains("active")) {
      if (customColorMode === "selection" && selColorBtn) {
        placeCustomColorPanelUnder(selColorBtn);
      } else if (customColorMode === "selectionFill" && selFillBtn) {
        placeCustomColorPanelUnder(selFillBtn);
      }
    }
  }

  function updateSelectionToolbar() {
    const selectMode = canEdit && currentTool === "select";
    const { objects, anchor } = resolveSelectionAnchor();

    if (!selectionToolbar) return;
    if (!selectMode || !objects.length || !anchor) {
      selectionToolbar.classList.remove("active");
      closeSelPopovers();
      updateUndoRedoDockVisibility();
      return;
    }

    const colorObjs = objects.filter(hasColorSupport);
    const fillObjs = objects.filter(isFillableShapeObject);
    const strokeObjs = objects.filter(hasStrokeWidthSupport);
    const cornerObjs = objects.filter((o) => isCornerRadiusSupportedObject(o));
    const lockEligible = objects.every((o) => isLockEligibleObject(o));
    const allLocked = lockEligible && objects.every((o) => isObjectLocked(o));
    // Rotation needs a real Fabric active object/selection to call .rotate()
    // on - a "focused locked object" anchor (see resolveSelectionAnchor) is
    // just a stand-in for showing the unlock affordance and isn't actually
    // selected in Fabric, so rotating it isn't possible until unlocked.
    const rotateEligible = anchor === fabricCanvas.getActiveObject();

    setSelBtnVisible(selColorBtn, colorObjs.length > 0);
    setSelBtnVisible(selFillBtn, fillObjs.length > 0);
    setSelBtnVisible(selStrokeBtn, strokeObjs.length > 0);
    setSelBtnVisible(selCornerBtn, cornerObjs.length > 0);
    setSelBtnVisible(selLockBtn, lockEligible);
    setSelBtnVisible(selRotateBtn, rotateEligible);
    const hasLeadingIcon = colorObjs.length || fillObjs.length || strokeObjs.length || cornerObjs.length || lockEligible || rotateEligible;
    if (selDeleteSep) selDeleteSep.classList.toggle("is-hidden", !hasLeadingIcon);

    if (colorObjs.length) {
      const color = getObjectDisplayColor(colorObjs[0]);
      if (selColorDot) selColorDot.style.background = color;
      buildSelectionPalette(color);
    }
    if (fillObjs.length) {
      const fill = getObjectFillValue(fillObjs[0]);
      if (selFillBtn) selFillBtn.classList.toggle("active", !!fill);
      buildSelectionFillPalette(fill);
    }
    if (strokeObjs.length && selectionStrokeWidthEl) {
      const avg = strokeObjs.reduce((acc, o) => acc + getObjectStrokeWidthValue(o), 0) / strokeObjs.length;
      selectionStrokeWidthEl.value = String(Math.round(avg));
    }
    if (cornerObjs.length && cornerRadiusEl) {
      const avg = cornerObjs.reduce((acc, o) => acc + getObjectCornerRadius(o), 0) / cornerObjs.length;
      cornerRadiusEl.value = String(Math.round(avg));
    }
    if (selLockBtn) {
      selLockBtn.classList.toggle("active", allLocked);
      selLockBtn.title = allLocked ? "Разблокировать" : "Заблокировать";
      if (selLockIcon) {
        selLockIcon.style.setProperty("--icon", `url('/static/icons/${allLocked ? "lock" : "unlock"}-outline.svg')`);
      }
    }

    const shown = placeAboveAnchor(selectionToolbar, anchor);
    selectionToolbar.classList.toggle("active", shown);
    if (!shown) {
      closeSelPopovers();
    } else {
      repositionOpenSelPopovers();
    }
    updateUndoRedoDockVisibility();
  }

  function toggleSelectionLock() {
    const selectedObjects = resolveSelectionAnchor().objects;
    if (!canEdit || !selectedObjects.length || !selectedObjects.every((obj) => isLockEligibleObject(obj))) return;
    const allLocked = selectedObjects.every((obj) => isObjectLocked(obj));
    const nextLocked = !allLocked;
    selectedObjects.forEach((obj) => {
      setObjectLocked(obj, nextLocked);
      enqueueUpdateOp(obj);
    });
    if (nextLocked) {
      focusedLockedObject = selectedObjects.length === 1 ? selectedObjects[0] : null;
      fabricCanvas.discardActiveObject();
    } else if (selectedObjects.length === 1 && isObjectOnCanvas(selectedObjects[0])) {
      focusedLockedObject = null;
      fabricCanvas.setActiveObject(selectedObjects[0]);
      applySelectionStyles();
    } else {
      focusedLockedObject = null;
    }
    syncObjectInteractivity();
    updateStylePanelVisibility();
    updateSelectionToolbar();
    fabricCanvas.requestRenderAll();
  }

  function applyEditPermissions() {
    // A read-only viewer has no use for drawing tools at all - hiding them
    // outright reads as "this board isn't yours to draw on" much more
    // clearly than a row of greyed-out buttons that look like a loading
    // glitch. Select/hand stay visible since viewers can still pan/inspect.
    document.querySelectorAll(".tool-btn[data-tool]").forEach((btn) => {
      const t = btn.dataset.tool;
      const isDrawingTool = t !== "select" && t !== "hand";
      btn.style.display = !canEdit && isDrawingTool ? "none" : "";
    });
    imageToolBtn.style.display = canEdit ? "" : "none";
    if (miroImportBtn) miroImportBtn.style.display = canEdit ? "" : "none";
    // Not a .tool-btn[data-tool] (it has its own click handler that both sets
    // the shape type and switches tool, rather than the generic per-data-tool
    // one), so it's excluded from the loop above and needs the same gating
    // applied explicitly - otherwise it stayed visible while every other
    // drawing tool was correctly hidden for a read-only viewer.
    if (stickerToolBtn) stickerToolBtn.style.display = canEdit ? "" : "none";
    // If permission was revoked while a now-hidden drawing tool was active
    // (e.g. a live role_update), fall back to select rather than leaving an
    // invisible tool "active" with no visible button to show for it.
    if (!canEdit && currentTool !== "select" && currentTool !== "hand") {
      setTool("select");
    }
    fabricCanvas.isDrawingMode = canEdit && currentTool === "pencil";
    fabricCanvas.selection = canEdit && currentTool === "select";
    fabricCanvas.skipTargetFind = !(canEdit && (currentTool === "select" || currentTool === "eraser" || currentTool === "text"));
    syncObjectInteractivity();
    updateSelectionToolbar();
    updateRotateButtonState();
  }

  function setBackground(mode) {
    currentBackground = mode;
    if (mode === "grid") {
      boardWrap.style.backgroundImage = GRID_BG_IMAGE;
      boardWrap.style.backgroundColor = "#ffffff";
      bgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/grid-3x3-gap-fill.svg\')"></span>';
    } else {
      boardWrap.style.backgroundImage = "none";
      boardWrap.style.backgroundColor = "#ffffff";
      bgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/border-all.svg\')"></span>';
    }
    updateGridPosition();
  }

  function ensureCanvasTransparentBackground() {
    fabricCanvas.backgroundColor = "rgba(0,0,0,0)";
    fabricCanvas.requestRenderAll();
  }

  function toggleBackground() {
    setBackground(currentBackground === "grid" ? "white" : "grid");
  }

  function normalizeHexColor(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (/^#[0-9a-f]{6}$/.test(raw)) return raw;
    if (/^#[0-9a-f]{3}$/.test(raw)) {
      return `#${raw[1]}${raw[1]}${raw[2]}${raw[2]}${raw[3]}${raw[3]}`;
    }
    return "";
  }

  function clamp01(value) {
    return Math.max(0, Math.min(1, Number(value) || 0));
  }

  function clamp255(value) {
    return Math.max(0, Math.min(255, Math.round(Number(value) || 0)));
  }

  function rgbToHex(r, g, b) {
    const toHex = (n) => clamp255(n).toString(16).padStart(2, "0");
    return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
  }

  function hexToRgb(value) {
    const hex = normalizeHexColor(value);
    if (!hex) return null;
    return {
      r: Number.parseInt(hex.slice(1, 3), 16),
      g: Number.parseInt(hex.slice(3, 5), 16),
      b: Number.parseInt(hex.slice(5, 7), 16),
    };
  }

  function hsvToRgb(h, s, v) {
    const hue = ((Number(h) % 360) + 360) % 360;
    const sat = clamp01(s);
    const val = clamp01(v);
    const c = val * sat;
    const x = c * (1 - Math.abs((hue / 60) % 2 - 1));
    const m = val - c;
    let rp = 0;
    let gp = 0;
    let bp = 0;
    if (hue < 60) {
      rp = c; gp = x; bp = 0;
    } else if (hue < 120) {
      rp = x; gp = c; bp = 0;
    } else if (hue < 180) {
      rp = 0; gp = c; bp = x;
    } else if (hue < 240) {
      rp = 0; gp = x; bp = c;
    } else if (hue < 300) {
      rp = x; gp = 0; bp = c;
    } else {
      rp = c; gp = 0; bp = x;
    }
    return {
      r: Math.round((rp + m) * 255),
      g: Math.round((gp + m) * 255),
      b: Math.round((bp + m) * 255),
    };
  }

  function rgbToHsv(r, g, b) {
    const rr = clamp255(r) / 255;
    const gg = clamp255(g) / 255;
    const bb = clamp255(b) / 255;
    const max = Math.max(rr, gg, bb);
    const min = Math.min(rr, gg, bb);
    const delta = max - min;
    let h = 0;
    if (delta !== 0) {
      if (max === rr) h = ((gg - bb) / delta) % 6;
      else if (max === gg) h = (bb - rr) / delta + 2;
      else h = (rr - gg) / delta + 4;
      h *= 60;
      if (h < 0) h += 360;
    }
    const s = max === 0 ? 0 : delta / max;
    const v = max;
    return { h, s, v };
  }

  function renderCustomColorPicker() {
    const hueColor = hsvToRgb(customColorState.h, 1, 1);
    const hueHex = rgbToHex(hueColor.r, hueColor.g, hueColor.b);
    if (customColorSv) customColorSv.style.backgroundColor = hueHex;

    if (customColorSv && customColorSvCursor) {
      const rect = customColorSv.getBoundingClientRect();
      const x = customColorState.s * rect.width;
      const y = (1 - customColorState.v) * rect.height;
      customColorSvCursor.style.left = `${x}px`;
      customColorSvCursor.style.top = `${y}px`;
    }
    if (customColorHue && customColorHueCursor) {
      const rect = customColorHue.getBoundingClientRect();
      const x = (customColorState.h / 360) * rect.width;
      customColorHueCursor.style.left = `${x}px`;
    }
  }

  function syncCustomInputsFromState() {
    renderCustomColorPicker();
  }

  function setCustomColorStateFromHex(hexValue) {
    const rgb = hexToRgb(hexValue);
    if (!rgb) return false;
    const hsv = rgbToHsv(rgb.r, rgb.g, rgb.b);
    customColorState.h = hsv.h;
    customColorState.s = hsv.s;
    customColorState.v = hsv.v;
    syncCustomInputsFromState();
    return true;
  }

  function colorFromCustomStateHex() {
    const rgb = hsvToRgb(customColorState.h, customColorState.s, customColorState.v);
    return rgbToHex(rgb.r, rgb.g, rgb.b);
  }

  function syncCustomColorInputs(sourceColor = currentColor) {
    const normalized = normalizeHexColor(sourceColor || currentColor);
    if (!normalized) return;
    setCustomColorStateFromHex(normalized);
  }

  function applyChosenColor(nextColor, preservePickerState = false) {
    const normalized = normalizeHexColor(nextColor);
    if (!normalized) return;
    currentColor = normalized;
    buildPalette();
    if (!preservePickerState) {
      syncCustomColorInputs(normalized);
    }
    updateBrush();
    applyStyleToSelection();
  }

  // Routes a live drag on the shared SV/hue picker to whichever thing opened
  // it - see customColorMode above.
  function applyCustomColorPick(hex) {
    if (customColorMode === "selection") {
      applySelectionColor(hex);
      if (selColorDot) selColorDot.style.background = hex;
    } else if (customColorMode === "selectionFill") {
      applySelectionFill(hex);
      if (selFillBtn) selFillBtn.classList.add("active");
    } else {
      applyChosenColor(hex, true);
    }
  }

  function buildPalette() {
    palette.innerHTML = "";
    for (const color of paletteColors) {
      const btn = document.createElement("button");
      btn.className = `swatch${color === currentColor ? " active" : ""}`;
      btn.style.background = color;
      btn.type = "button";
      btn.title = color;
      btn.addEventListener("click", () => {
        applyChosenColor(color);
      });
      palette.appendChild(btn);
    }

    const customBtn = document.createElement("button");
    const usingCustomColor = !paletteColors.includes(currentColor);
    customBtn.className = `swatch swatch-custom-trigger${usingCustomColor ? " active" : ""}`;
    customBtn.type = "button";
    customBtn.setAttribute("aria-label", "Открыть расширенную палитру");
    customBtn.title = "Выбрать свой цвет";
    customBtn.innerHTML = "";
    const openCustomColorPanel = (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (!customColorPanel) return;
      const isOpen = customColorPanel.classList.contains("active") && customColorMode === "pencil";
      if (isOpen) {
        customColorPanel.classList.remove("active");
        return;
      }
      customColorMode = "pencil";
      syncCustomColorInputs(currentColor);
      customColorPanel.classList.add("active");
      requestAnimationFrame(() => {
        placeCustomColorPanelUnder(customBtn);
        renderCustomColorPicker();
      });
    };
    customBtn.addEventListener("click", openCustomColorPanel);
    palette.appendChild(customBtn);
  }

  function updateBrush() {
    fabricCanvas.freeDrawingBrush = new fabric.PencilBrush(fabricCanvas);
    fabricCanvas.freeDrawingBrush.color = currentColor;
    fabricCanvas.freeDrawingBrush.width = Number(strokeWidthEl.value);
    fabricCanvas.freeDrawingBrush.strokeLineCap = "round";
    fabricCanvas.freeDrawingBrush.strokeLineJoin = "round";
    fabricCanvas.freeDrawingBrush.decimate = 1.1;
  }

  function setZoomLabel() {
    zoomCenterBtn.textContent = `${Math.round(fabricCanvas.getZoom() * 100)}%`;
  }

  function hidePanels() {
    pencilPanel.classList.remove("active");
    if (customColorPanel) customColorPanel.classList.remove("active");
    textPanel.classList.remove("active");
    if (shapePanel) shapePanel.classList.remove("active");
  }

  function placeCustomColorPanelUnder(anchorEl) {
    if (!customColorPanel || !anchorEl) return;
    const r = anchorEl.getBoundingClientRect();
    const panelW = customColorPanel.offsetWidth || 220;
    const panelH = customColorPanel.offsetHeight || 180;
    const left = Math.max(8, Math.min(window.innerWidth - panelW - 8, r.left + r.width / 2 - panelW / 2));
    const top = Math.max(8, Math.min(window.innerHeight - panelH - 8, r.bottom + 8));
    customColorPanel.style.left = `${left}px`;
    customColorPanel.style.top = `${top}px`;
  }

  function placePanelNear(panel, anchorEl) {
    const r = anchorEl.getBoundingClientRect();
    const panelW = panel.offsetWidth || 220;
    const panelH = panel.offsetHeight || 160;
    const tryTop = r.top - panelH - 10;
    const top = tryTop >= 8 ? tryTop : Math.min(window.innerHeight - panelH - 8, r.bottom + 8);
    const left = Math.max(8, Math.min(window.innerWidth - panelW - 8, r.left - panelW / 2 + r.width / 2));
    panel.style.left = `${left}px`;
    panel.style.top = `${top}px`;
  }

  function togglePanel(panel, anchorEl) {
    const active = panel.classList.contains("active");
    hidePanels();
    if (!active) {
      panel.classList.add("active");
      placePanelNear(panel, anchorEl);
    }
  }

  function setTool(tool) {
    currentTool = tool;
    document.querySelectorAll(".tool-btn[data-tool]").forEach((btn) => {
      // Stickers are implemented as currentTool "shape" + currentShapeType
      // "sticker" internally, but have their own dedicated toolbar button -
      // without this exception the generic data-tool match would light up
      // the "Фигуры" button too whenever a sticker is the active tool.
      const isSticker = btn === shapeToolBtn && currentShapeType === "sticker";
      btn.classList.toggle("active", !isSticker && btn.dataset.tool === currentTool);
    });
    if (stickerToolBtn) {
      stickerToolBtn.classList.toggle("active", currentTool === "shape" && currentShapeType === "sticker");
    }

    fabricCanvas.isDrawingMode = canEdit && currentTool === "pencil";
    fabricCanvas.selection = canEdit && currentTool === "select";
    fabricCanvas.skipTargetFind = !(canEdit && (currentTool === "select" || currentTool === "eraser" || currentTool === "text"));

    if (currentTool === "hand") {
      fabricCanvas.defaultCursor = "grab";
    } else if (currentTool === "eraser") {
      fabricCanvas.defaultCursor = "cell";
    } else {
      fabricCanvas.defaultCursor = "default";
    }

    syncObjectInteractivity();
    updateBrush();
  }

  function suppressTouchInput(ms = 180) {
    suppressTouchInputUntil = Date.now() + ms;
  }

  function suppressPathCreated(ms = 600) {
    suppressPathCreatedUntil = Date.now() + ms;
  }

  function isTouchLikeEvent(evt) {
    if (!evt) return false;
    if (evt.pointerType === "touch") return true;
    if (evt.type && String(evt.type).startsWith("touch")) return true;
    if (evt.touches || evt.changedTouches) return true;
    return false;
  }

  function getClientPoint(evt) {
    if (!evt) return null;
    if (Number.isFinite(evt.clientX) && Number.isFinite(evt.clientY)) {
      return { x: evt.clientX, y: evt.clientY };
    }
    const touch =
      (evt.touches && evt.touches[0]) ||
      (evt.changedTouches && evt.changedTouches[0]) ||
      null;
    if (touch && Number.isFinite(touch.clientX) && Number.isFinite(touch.clientY)) {
      return { x: touch.clientX, y: touch.clientY };
    }
    return null;
  }

  function isTouchInputSuppressed(evt) {
    const touchCount = evt && evt.touches ? evt.touches.length : 0;
    return pinchMode || touchCount > 1 || Date.now() < suppressTouchInputUntil;
  }

  function cancelTransientCanvasActions() {
    panMode = false;
    erasing = false;
    if (drawingShape) {
      fabricCanvas.remove(drawingShape);
      drawingShape = null;
      drawingStart = null;
    }
    fabricCanvas.discardActiveObject();
    fabricCanvas.requestRenderAll();
    drawMiniMap();
    updateSelectionToolbar();
  }

  function startPinchWithPoints(p1, p2) {
    if (!p1 || !p2) return;
    pinchMode = true;
    suppressTouchInput(320);
    suppressPathCreated(1800);

    const active = fabricCanvas.getActiveObject();
    if (active && active.type === "i-text" && typeof active.exitEditing === "function") {
      const isEmptyEditingText = !!active.isEditing && String(active.text || "").trim() === "";
      active.exitEditing();
      if (isEmptyEditingText) {
        fabricCanvas.remove(active);
      }
    }

    cancelTransientCanvasActions();

    pinchDist = Math.hypot(p2.x - p1.x, p2.y - p1.y);
    pinchCenter = { x: (p1.x + p2.x) / 2, y: (p1.y + p2.y) / 2 };

    // В pinch-режиме полностью отключаем интерактив Fabric, чтобы не рисовать/не выделять случайно.
    fabricCanvas.isDrawingMode = false;
    fabricCanvas.selection = false;
    fabricCanvas.skipTargetFind = true;
  }

  function startPinch(touches) {
    if (!touches || touches.length < 2) return;
    const [t1, t2] = touches;
    startPinchWithPoints({ x: t1.clientX, y: t1.clientY }, { x: t2.clientX, y: t2.clientY });
  }

  function stopPinch() {
    if (!pinchMode) return;
    pinchMode = false;
    pinchCenter = null;
    suppressTouchInput(320);
    suppressPathCreated(700);
    setTool(currentTool);
  }

  function updatePinchWithPoints(p1, p2) {
    if (!pinchMode || !p1 || !p2 || !pinchCenter) return;
    const dist = Math.hypot(p2.x - p1.x, p2.y - p1.y);
    const center = { x: (p1.x + p2.x) / 2, y: (p1.y + p2.y) / 2 };

    const factor = dist / Math.max(1, pinchDist);
    const rect = boardWrap.getBoundingClientRect();
    zoomByFactor(factor, center.x - rect.left, center.y - rect.top);

    const v = fabricCanvas.viewportTransform;
    v[4] += center.x - pinchCenter.x;
    v[5] += center.y - pinchCenter.y;
    const active = fabricCanvas.getActiveObject();
    if (active && typeof active.setCoords === "function") active.setCoords();
    fabricCanvas.requestRenderAll();

    pinchDist = dist;
    pinchCenter = center;
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
    updateSelectionToolbar();
  }

  function setShapeType(shape) {
    currentShapeType = shape;
    document.querySelectorAll(".shape-btn[data-shape]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.shape === currentShapeType);
    });
    // Sticker lives on its own dedicated toolbar button now (not the shape
    // dropdown), so it gets its own active-state toggle instead of taking
    // over the "Фигуры" button's icon.
    if (stickerToolBtn) stickerToolBtn.classList.toggle("active", currentShapeType === "sticker");
    if (shapeToolBtn && shape !== "sticker") {
      const shapeIconByType = {
        rect: '<span class="bi-local" style="--icon:url(\'/static/icons/square.svg\')"></span>',
        ellipse: '<span class="bi-local" style="--icon:url(\'/static/icons/circle.svg\')"></span>',
        line: '<span class="bi-local" style="--icon:url(\'/static/icons/slash-lg.svg\')"></span>',
        arrow: '<span class="shape-arrow-glyph">↗</span>',
        triangle: '<span class="bi-local" style="--icon:url(\'/static/icons/triangle.svg\')"></span>',
        diamond: '<span class="bi-local" style="--icon:url(\'/static/icons/diamond.svg\')"></span>',
      };
      shapeToolBtn.innerHTML = shapeIconByType[currentShapeType] || shapeIconByType.rect;
    }
  }

  function updateUndoRedoDockVisibility() {
    // The selection mini-toolbar floats above the object itself now (not
    // fixed bottom-left like the old panel), so it no longer competes for
    // space with the undo/redo dock - always show it.
    if (!undoRedoDock) return;
    undoRedoDock.classList.remove("is-hidden");
  }

  function syncObjectInteractivity() {
    const selectMode = canEdit && currentTool === "select";
    const eraseMode = canEdit && currentTool === "eraser";
    const textMode = canEdit && currentTool === "text";

    fabricCanvas.forEachObject((obj) => {
      const locked = isObjectLocked(obj);
      const textSelectable = textMode && !locked && isTextModeSelectableObject(obj);
      const textEditable = textMode && isTextObject(obj);
      obj.selectable = (selectMode && !locked) || textSelectable;
      obj.evented = locked ? (selectMode || textSelectable) : (selectMode || eraseMode || textSelectable || textEditable);
    });

    if (!selectMode && !textMode) {
      focusedLockedObject = null;
      fabricCanvas.discardActiveObject();
    }

    updateRotateButtonState();
    updateSelectionToolbar();
    fabricCanvas.requestRenderAll();
  }

  function applySelectionStyles() {
    const active = fabricCanvas.getActiveObject();
    updateRotateButtonState();
    if (!active) return;

    if (active.type === "activeSelection") {
      active.set({
        hasControls: true,
        hasBorders: true,
        borderColor: "#16a34a",
        borderDashArray: [6, 3],
        lockRotation: true,
        cornerStyle: "rect",
        transparentCorners: false,
        cornerColor: "#ffffff",
        cornerStrokeColor: "#16a34a",
        cornerSize: 10,
        touchCornerSize: 28,
        lockUniScaling: false,
      });
      setCornerOnlyControls(active);
    } else {
      const uniformScale = isUniformScaleObject(active);
      active.set({
        hasControls: true,
        hasBorders: true,
        borderColor: "#16a34a",
        borderDashArray: [6, 3],
        cornerColor: "#ffffff",
        cornerStrokeColor: "#16a34a",
        transparentCorners: false,
        cornerStyle: "rect",
        cornerSize: 10,
        touchCornerSize: 28,
        lockRotation: true,
        lockUniScaling: uniformScale,
      });
      setCornerOnlyControls(active);
    }

    enforceNoMirrorInTarget(active);
    fabricCanvas.requestRenderAll();
  }

  function resizeCanvas(initial = false) {
    const prev = fabricCanvas.viewportTransform ? [...fabricCanvas.viewportTransform] : null;
    const rect = boardWrap.getBoundingClientRect();
    fabricCanvas.setDimensions({ width: rect.width, height: rect.height });
    if (initial || !prev) {
      resetView();
      return;
    }
    fabricCanvas.setViewportTransform(prev);
    fabricCanvas.requestRenderAll();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
  }

  function worldFromScreen(x, y) {
    const p = new fabric.Point(x, y);
    return fabric.util.transformPoint(p, fabric.util.invertTransform(fabricCanvas.viewportTransform));
  }

  function screenFromWorld(x, y) {
    const p = fabric.util.transformPoint(new fabric.Point(x, y), fabricCanvas.viewportTransform);
    return { x: p.x, y: p.y };
  }

  function centerOnWorld(wx, wy) {
    const v = fabricCanvas.viewportTransform;
    const z = fabricCanvas.getZoom();
    v[4] = fabricCanvas.getWidth() / 2 - wx * z;
    v[5] = fabricCanvas.getHeight() / 2 - wy * z;
    const active = fabricCanvas.getActiveObject();
    if (active && typeof active.setCoords === "function") active.setCoords();
    fabricCanvas.requestRenderAll();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
    updateSelectionToolbar();
  }

  function resetView() {
    const w = fabricCanvas.getWidth();
    const h = fabricCanvas.getHeight();
    fabricCanvas.setViewportTransform([1, 0, 0, 1, w / 2, h / 2]);
    const active = fabricCanvas.getActiveObject();
    if (active && typeof active.setCoords === "function") active.setCoords();
    fabricCanvas.requestRenderAll();
    setZoomLabel();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
    updateSelectionToolbar();
  }

  function zoomByFactor(factor, centerX, centerY) {
    let zoom = fabricCanvas.getZoom() * factor;
    zoom = Math.max(0.08, Math.min(24, zoom));
    const p = new fabric.Point(centerX, centerY);
    fabricCanvas.zoomToPoint(p, zoom);
    const active = fabricCanvas.getActiveObject();
    if (active && typeof active.setCoords === "function") active.setCoords();
    setZoomLabel();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
    updateSelectionToolbar();
  }

  function updateGridPosition() {
    if (currentBackground !== "grid") return;
    const v = fabricCanvas.viewportTransform || [1, 0, 0, 1, 0, 0];
    const zoom = Math.max(0.0001, fabricCanvas.getZoom());

    // LOD сетка: при отдалении объединяем клетки по 2x2, 4x4, 8x8...
    const mergeLevel = Math.max(0, Math.floor(Math.log2(1 / zoom)));
    const mergeFactor = 2 ** mergeLevel;

    // Мировой шаг сетки и его экранный размер в пикселях.
    const worldStep = GRID_WORLD_SIZE * mergeFactor;
    const screenStep = Math.max(4, Math.round(worldStep * zoom));

    const ox = ((v[4] % screenStep) + screenStep) % screenStep;
    const oy = ((v[5] % screenStep) + screenStep) % screenStep;
    boardWrap.style.backgroundSize = `${screenStep}px ${screenStep}px`;
    boardWrap.style.backgroundPosition = `${ox}px ${oy}px`;
  }

  function ensureObjMeta(obj) {
    if (!obj.obj_id) obj.obj_id = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
    if (!obj.author_id) obj.author_id = myUserId || "unknown";
    if (!obj.author_name) obj.author_name = localUsername;
  }

  function getObjectCornerRadius(obj) {
    if (obj.type === "image") return Number(obj.cornerRadius || 0);
    return Number(obj.cornerRadius || obj.rx || 0);
  }

  function isCornerRadiusSupportedObject(obj) {
    return !!(obj && (obj.type === "rect" || obj.type === "image"));
  }

  function applyImageCornerRadius(img, radiusPx) {
    const w = Math.max(1, Number(img.width || 1));
    const h = Math.max(1, Number(img.height || 1));
    const sx = Math.max(0.0001, Math.abs(Number(img.scaleX || 1)));
    const sy = Math.max(0.0001, Math.abs(Number(img.scaleY || 1)));
    const maxPx = Math.min(img.getScaledWidth() / 2, img.getScaledHeight() / 2);
    const safePx = Math.max(0, Math.min(maxPx, Number(radiusPx) || 0));

    // Храним радиус в "экранных" пикселях: визуально предсказуемо для пользователя.
    img.cornerRadius = safePx;
    if (safePx <= 0) {
      img.clipPath = null;
      img.dirty = true;
      return;
    }

    // clipPath живет в локальных координатах объекта, поэтому переводим px в локальные единицы.
    const rxLocal = Math.max(0, Math.min(w / 2, safePx / sx));
    const ryLocal = Math.max(0, Math.min(h / 2, safePx / sy));
    img.clipPath = new fabric.Rect({
      width: w,
      height: h,
      rx: rxLocal,
      ry: ryLocal,
      originX: "center",
      originY: "center",
    });
    img.dirty = true;
  }

  function setObjectCornerRadius(obj, radius) {
    if (obj.type === "rect") {
      const maxR = Math.min((obj.width || 1) / 2, (obj.height || 1) / 2);
      const safe = Math.max(0, Math.min(maxR, radius));
      obj.cornerRadius = safe;
      obj.set({ rx: safe, ry: safe });
    } else if (obj.type === "image") {
      applyImageCornerRadius(obj, radius);
    }
  }

  function syncRoundedImages() {
    fabricCanvas.forEachObject((obj) => {
      if (obj.type === "image") {
        applyImageCornerRadius(obj, getObjectCornerRadius(obj));
      }
    });
  }

  function syncNoMirrorObjects() {
    fabricCanvas.forEachObject((obj) => {
      enforceNoMirrorObject(obj);
    });
  }


  function isMultiSelectionObject(obj) {
    return !!(
      obj
      && (
        (typeof obj.type === "string" && obj.type.toLowerCase() === "activeselection")
        || (typeof obj.getObjects === "function" && Array.isArray(obj.getObjects()) && obj.getObjects().length > 0)
      )
    );
  }

  // --- Contextual selection panel (color / stroke width / corner radius / delete) ---
  //
  // Deliberately uses its own getPanelSelectionObjects() instead of the older
  // getSelectionObjects(): that one treats ANY object with a getObjects()
  // method as a "multi selection" and flattens it into its children, which is
  // correct for a true multi-select (fabric.ActiveSelection) but wrong for a
  // single grouped object (arrow, sticker) - flattening a sticker would expose
  // its internal rect/textbox as if they were independent top-level objects.
  const STROKE_SHAPE_TYPES = new Set(["rect", "ellipse", "triangle", "path", "line", "polyline", "polygon"]);
  // Only the closed, "solid" shapes get a fill toggle - a line has no
  // interior to fill, and diamond is implemented as a plain "polygon" (the
  // only top-level polygon shape in the app, since the arrow's triangle head
  // lives inside a group and isn't independently selectable).
  const FILLABLE_SHAPE_TYPES = new Set(["rect", "ellipse", "triangle", "polygon"]);

  function isActiveSelectionObject(obj) {
    return !!(obj && typeof obj.type === "string" && obj.type.toLowerCase() === "activeselection");
  }

  function getPanelSelectionObjects() {
    const active = fabricCanvas.getActiveObject();
    if (!active) return [];
    return isActiveSelectionObject(active) ? active.getObjects() : [active];
  }

  function isStickerObject(obj) {
    return !!(obj && obj.type === "stickynote");
  }

  function isArrowGroupObject(obj) {
    return !!(obj && obj.type === "group");
  }

  // Editing text inside a Fabric Group doesn't work out of the box - Fabric's
  // native double-click-to-edit only fires on a directly-selected IText/Textbox,
  // not a subTargetCheck:false group's children. Rather than fight Fabric's
  // group/subtarget editing internals, overlay a plain HTML <textarea> on top
  // of the sticker (positioned via the same screen-space conversion already
  // used for the selection lock button) and write the result back on exit.
  // Shrinks the overlay's font size (down to STICKER_MIN_FONT_SIZE, scaled
  // for the current zoom/sticker size) until the typed text stops
  // overflowing the box, and grows it back toward the sticker's normal size
  // as text is deleted - the same "auto-fit while typing" behavior Miro uses
  // for sticky notes, rather than letting text overflow or wrap-clip.
  // Measures wrapped text height off-screen, independent of the live
  // overlay's own padding/height - letting the overlay's padding itself be
  // the thing we solve for (see fitStickerOverlayFont) without a feedback
  // loop from mutating the element we're also measuring.
  function measureStickerTextHeight(text, fontPx, widthPx) {
    if (!stickerMeasureProbe) {
      stickerMeasureProbe = document.createElement("div");
      stickerMeasureProbe.style.position = "fixed";
      stickerMeasureProbe.style.visibility = "hidden";
      stickerMeasureProbe.style.left = "-9999px";
      stickerMeasureProbe.style.top = "0";
      stickerMeasureProbe.style.whiteSpace = "pre-wrap";
      stickerMeasureProbe.style.wordBreak = "break-word";
      stickerMeasureProbe.style.fontFamily = "'Montserrat', 'Segoe UI', sans-serif";
      stickerMeasureProbe.style.fontWeight = "500";
      stickerMeasureProbe.style.lineHeight = "1.3";
      document.body.appendChild(stickerMeasureProbe);
    }
    stickerMeasureProbe.style.width = `${Math.max(1, widthPx)}px`;
    stickerMeasureProbe.style.fontSize = `${fontPx}px`;
    stickerMeasureProbe.textContent = text.length ? text : " ";
    return stickerMeasureProbe.scrollHeight;
  }

  function fitStickerOverlayFont() {
    if (!stickerEditOverlay || stickerEditOverlay.style.display === "none") return;
    const displayScale = Number(stickerEditOverlay.dataset.displayScale) || 1;
    const maxFontPx = STICKER_DEFAULT_FONT_SIZE * displayScale;
    const minFontPx = STICKER_MIN_FONT_SIZE * displayScale;
    const pad = 14; // matches #stickerEditOverlay's own CSS padding
    const boxW = stickerEditOverlay.clientWidth;
    const boxH = stickerEditOverlay.clientHeight;
    const contentW = Math.max(1, boxW - pad * 2);
    const text = stickerEditOverlay.value;

    let fontPx = maxFontPx;
    let contentH = measureStickerTextHeight(text, fontPx, contentW);
    while (fontPx > minFontPx && contentH + pad * 2 > boxH) {
      fontPx -= 1;
      contentH = measureStickerTextHeight(text, fontPx, contentW);
    }
    stickerEditOverlay.style.fontSize = `${fontPx}px`;

    // Vertically center by splitting whatever room is left over (after the
    // fitted text and its fixed side padding) evenly above and below -
    // horizontal centering is a plain CSS text-align, but there's no CSS
    // equivalent for a <textarea>'s own vertical text position.
    const extra = Math.max(0, boxH - pad * 2 - contentH);
    const topPad = pad + extra / 2;
    stickerEditOverlay.style.paddingTop = `${topPad}px`;
    stickerEditOverlay.style.paddingBottom = `${topPad}px`;
  }

  function enterStickerEditMode(note) {
    if (!stickerEditOverlay || !canEdit) return;
    fabricCanvas.discardActiveObject();
    note.setCoords();
    const bounds = note.getBoundingRect();
    const canvasRect = canvasEl.getBoundingClientRect();
    const topLeft = screenFromWorld(bounds.left, bounds.top);
    const bottomRight = screenFromWorld(bounds.left + bounds.width, bounds.top + bounds.height);
    // getBoundingRect() bakes the note's own scaleX/scaleY into its width/
    // height already, but not the viewport zoom - convert both corners
    // through the viewport transform so the overlay box (and the font-size
    // below) match what's actually rendered on screen at any zoom level.
    const zoom = fabricCanvas.getZoom();
    const displayScale = (Number(note.scaleX) || 1) * zoom;

    stickerEditOverlay.value = note.noteText || "";
    stickerEditOverlay.style.left = `${canvasRect.left + topLeft.x}px`;
    stickerEditOverlay.style.top = `${canvasRect.top + topLeft.y}px`;
    stickerEditOverlay.style.width = `${bottomRight.x - topLeft.x}px`;
    stickerEditOverlay.style.height = `${bottomRight.y - topLeft.y}px`;
    stickerEditOverlay.dataset.displayScale = String(displayScale);
    // Match the sticker's own fill so the editing textarea reads as "the
    // sticker itself, now editable" rather than a generic input box floating
    // over a gap where the note briefly disappeared.
    stickerEditOverlay.style.background = note.fill || "#fef08a";
    stickerEditOverlay.style.display = "block";
    stickerEditOverlay.dataset.targetObjId = note.obj_id || "";

    note.set({ opacity: 0 });
    fabricCanvas.requestRenderAll();
    fitStickerOverlayFont();
    requestAnimationFrame(() => {
      stickerEditOverlay.focus();
      stickerEditOverlay.select();
    });
  }

  function exitStickerEditMode(commit) {
    if (!stickerEditOverlay || stickerEditOverlay.style.display === "none") return;
    const objId = stickerEditOverlay.dataset.targetObjId || "";
    const value = stickerEditOverlay.value;
    const displayScale = Number(stickerEditOverlay.dataset.displayScale) || 1;
    const fittedFontPx = parseFloat(stickerEditOverlay.style.fontSize) || (STICKER_DEFAULT_FONT_SIZE * displayScale);
    stickerEditOverlay.style.display = "none";
    stickerEditOverlay.dataset.targetObjId = "";

    const note = objId ? findObjectById(objId) : null;
    if (!note) return;
    note.set({ opacity: 1 });
    if (commit) {
      // Store the font size back in the sticker's own (unscaled, un-zoomed)
      // units so it renders at the same visual size next time, regardless of
      // the zoom level or scale in effect while editing.
      const nextFontSize = Math.max(STICKER_MIN_FONT_SIZE, fittedFontPx / displayScale);
      const textChanged = note.noteText !== value;
      const fontChanged = Math.abs((Number(note.noteFontSize) || 0) - nextFontSize) > 0.5;
      if (textChanged || fontChanged) {
        note.set({ noteText: value, noteFontSize: nextFontSize });
        note.dirty = true;
        enqueueUpdateOp(note);
      }
    }
    fabricCanvas.requestRenderAll();
  }

  function hasColorSupport(obj) {
    if (!obj) return false;
    if (obj.type === "i-text") return true;
    if (isStickerObject(obj)) return true;
    if (isArrowGroupObject(obj)) return true;
    return STROKE_SHAPE_TYPES.has(obj.type);
  }

  function hasStrokeWidthSupport(obj) {
    if (!obj) return false;
    if (isStickerObject(obj)) return false;
    if (isArrowGroupObject(obj)) return true;
    return STROKE_SHAPE_TYPES.has(obj.type);
  }

  function getObjectDisplayColor(obj) {
    if (!obj) return paletteColors[0];
    if (obj.type === "i-text") return obj.fill || paletteColors[0];
    if (isStickerObject(obj)) {
      return obj.fill || STICKER_COLORS[0];
    }
    if (isArrowGroupObject(obj) && typeof obj.getObjects === "function") {
      const child = obj.getObjects().find((c) => c && c.stroke);
      return (child && child.stroke) || paletteColors[0];
    }
    return obj.stroke || paletteColors[0];
  }

  function getObjectStrokeWidthValue(obj) {
    if (!obj) return 2;
    if (isArrowGroupObject(obj) && typeof obj.getObjects === "function") {
      const child = obj.getObjects().find((c) => c && typeof c.strokeWidth === "number");
      return child ? Number(child.strokeWidth) : 2;
    }
    return Number(obj.strokeWidth) || 2;
  }

  function applyColorToObject(obj, color) {
    if (!hasColorSupport(obj)) return false;
    if (obj.type === "i-text") {
      obj.set({ fill: color });
      return true;
    }
    if (isStickerObject(obj)) {
      obj.set({ fill: color });
      obj.dirty = true;
      return true;
    }
    if (isArrowGroupObject(obj) && typeof obj.getObjects === "function") {
      obj.getObjects().forEach((child) => {
        if (!child || typeof child.set !== "function") return;
        child.set({ stroke: color });
        if (child.type === "polygon") child.set({ fill: color });
      });
      obj.dirty = true;
      return true;
    }
    obj.set({ stroke: color });
    return true;
  }

  function isFillableShapeObject(obj) {
    return !!obj && FILLABLE_SHAPE_TYPES.has(obj.type);
  }

  function getObjectFillValue(obj) {
    if (!isFillableShapeObject(obj)) return null;
    const fill = obj.fill;
    return fill && fill !== "transparent" ? fill : null;
  }

  function applyFillToObject(obj, color) {
    if (!isFillableShapeObject(obj)) return false;
    obj.set({ fill: color || "transparent" });
    return true;
  }

  function applyStrokeWidthToObject(obj, width) {
    if (!hasStrokeWidthSupport(obj)) return false;
    if (isArrowGroupObject(obj) && typeof obj.getObjects === "function") {
      obj.getObjects().forEach((child) => {
        if (child && typeof child.set === "function") child.set({ strokeWidth: width });
      });
      obj.dirty = true;
      return true;
    }
    obj.set({ strokeWidth: width });
    return true;
  }

  function enqueuePanelSelectionUpdates(objs) {
    const ops = objs
      .filter((obj) => isSyncableObject(obj))
      .map((obj) => buildAction("update", { object: serializeObject(obj) }));
    enqueueOps(ops);
  }

  function applySelectionColor(color) {
    const objs = getPanelSelectionObjects();
    let changed = false;
    objs.forEach((obj) => {
      if (applyColorToObject(obj, color)) changed = true;
    });
    if (!changed) return;
    fabricCanvas.requestRenderAll();
    enqueuePanelSelectionUpdates(objs);
    updateStylePanelVisibility();
  }

  function applySelectionFill(color) {
    const objs = getPanelSelectionObjects().filter(isFillableShapeObject);
    let changed = false;
    objs.forEach((obj) => {
      if (applyFillToObject(obj, color)) changed = true;
    });
    if (!changed) return;
    fabricCanvas.requestRenderAll();
    enqueuePanelSelectionUpdates(objs);
    updateStylePanelVisibility();
  }

  function applySelectionStrokeWidth(width) {
    const w = Number(width);
    if (!Number.isFinite(w)) return;
    const objs = getPanelSelectionObjects();
    let changed = false;
    objs.forEach((obj) => {
      if (applyStrokeWidthToObject(obj, w)) changed = true;
    });
    if (!changed) return;
    fabricCanvas.requestRenderAll();
    enqueuePanelSelectionUpdates(objs);
  }

  function buildSelectionPalette(activeColor) {
    if (!selectionPalette) return;
    const objs = getPanelSelectionObjects();
    const isStickerSelection = objs.length > 0 && objs.every(isStickerObject);
    const colors = isStickerSelection ? STICKER_COLORS : paletteColors;

    selectionPalette.innerHTML = "";
    for (const color of colors) {
      const btn = document.createElement("button");
      btn.className = `swatch${color === activeColor ? " active" : ""}`;
      btn.style.background = color;
      btn.type = "button";
      btn.title = color;
      btn.addEventListener("click", () => applySelectionColor(color));
      selectionPalette.appendChild(btn);
    }

    const usingCustom = !colors.includes(activeColor);
    const customBtn = document.createElement("button");
    customBtn.className = `swatch swatch-custom-trigger${usingCustom ? " active" : ""}`;
    customBtn.type = "button";
    customBtn.setAttribute("aria-label", "Открыть расширенную палитру");
    customBtn.title = "Выбрать свой цвет";
    customBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (!customColorPanel) return;
      const isOpen = customColorPanel.classList.contains("active") && customColorMode === "selection";
      if (isOpen) {
        customColorPanel.classList.remove("active");
        return;
      }
      customColorMode = "selection";
      syncCustomColorInputs(activeColor);
      customColorPanel.classList.add("active");
      requestAnimationFrame(() => {
        placeCustomColorPanelUnder(customBtn);
        renderCustomColorPicker();
      });
    });
    selectionPalette.appendChild(customBtn);
  }

  function buildSelectionFillPalette(activeFill) {
    if (!selectionFillPalette) return;
    selectionFillPalette.innerHTML = "";

    const noneBtn = document.createElement("button");
    noneBtn.className = `swatch swatch-none${activeFill ? "" : " active"}`;
    noneBtn.type = "button";
    noneBtn.title = "Без заливки";
    noneBtn.addEventListener("click", () => applySelectionFill(null));
    selectionFillPalette.appendChild(noneBtn);

    for (const color of paletteColors) {
      const btn = document.createElement("button");
      btn.className = `swatch${color === activeFill ? " active" : ""}`;
      btn.style.background = color;
      btn.type = "button";
      btn.title = color;
      btn.addEventListener("click", () => applySelectionFill(color));
      selectionFillPalette.appendChild(btn);
    }

    const usingCustom = !!activeFill && !paletteColors.includes(activeFill);
    const customBtn = document.createElement("button");
    customBtn.className = `swatch swatch-custom-trigger${usingCustom ? " active" : ""}`;
    customBtn.type = "button";
    customBtn.setAttribute("aria-label", "Открыть расширенную палитру");
    customBtn.title = "Выбрать свой цвет";
    customBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (!customColorPanel) return;
      const isOpen = customColorPanel.classList.contains("active") && customColorMode === "selectionFill";
      if (isOpen) {
        customColorPanel.classList.remove("active");
        return;
      }
      customColorMode = "selectionFill";
      syncCustomColorInputs(activeFill || paletteColors[0]);
      customColorPanel.classList.add("active");
      requestAnimationFrame(() => {
        placeCustomColorPanelUnder(customBtn);
        renderCustomColorPicker();
      });
    });
    selectionFillPalette.appendChild(customBtn);
  }

  // Kept as a thin alias: this used to own the old bottom-left panel and is
  // still called from many event handlers throughout the file. All of that
  // logic now lives in updateSelectionToolbar (the mini-toolbar above the
  // selection), so every one of those call sites gets the new behavior for
  // free without having to touch each one individually.
  function updateStylePanelVisibility() {
    updateSelectionToolbar();
  }

  function applyCornerRadiusToSelection(value) {
    const radius = Number(value);
    if (!Number.isFinite(radius)) return;
    // Uses resolveSelectionAnchor() rather than the older getSelectionObjects()
    // so this also works for a locked-but-focused image (which has no true
    // Fabric selection to flatten) and treats a grouped object as one unit
    // instead of exploding it into its children.
    const objs = resolveSelectionAnchor().objects.filter((o) => isCornerRadiusSupportedObject(o));
    if (!objs.length) return;
    objs.forEach((obj) => {
      setObjectCornerRadius(obj, radius);
      obj.setCoords();
    });
    fabricCanvas.requestRenderAll();
    enqueuePanelSelectionUpdates(objs);
  }

  function applyStyleToSelection() {
    const active = fabricCanvas.getActiveObject();
    if (!active) return;

    const strokeW = Number(strokeWidthEl.value);
    const applyOne = (obj) => {
      if (obj.type === "i-text") {
        obj.set({
          fill: currentColor,
          fontSize: Number(textSizeEl ? textSizeEl.value : obj.fontSize) || obj.fontSize,
          fontFamily: "Montserrat, sans-serif",
          fontWeight: "500",
        });
        return;
      }
      if (obj.type === "group" && typeof obj.getObjects === "function") {
        obj.getObjects().forEach((child) => {
          if (!child || typeof child.set !== "function") return;
          child.set({ stroke: currentColor, strokeWidth: strokeW });
          if (child.type === "polygon") child.set({ fill: currentColor });
        });
        return;
      }
      if (obj.type === "rect" || obj.type === "ellipse" || obj.type === "triangle" || obj.type === "path" || obj.type === "line" || obj.type === "polyline" || obj.type === "polygon") {
        obj.set({ stroke: currentColor, strokeWidth: strokeW });
      }
    };

    if (isActiveSelectionObject(active)) {
      active.getObjects().forEach(applyOne);
    } else {
      // applyOne already handles a single grouped object (arrow/sticker) by
      // iterating its own children internally - the old isMultiSelectionObject
      // check here treated ANY object with getObjects() (including a lone
      // sticker) as a multi-selection, which fed its rect/textbox children
      // into enqueueSelectionUpdates as independent top-level objects instead
      // of the group, corrupting the sticker into two loose synced objects.
      applyOne(active);
    }

    fabricCanvas.requestRenderAll();
    enqueueSelectionUpdates();
  }

  function serializeObject(obj) {
    ensureObjMeta(obj);
    return obj.toObject(["obj_id", "author_id", "author_name", "cornerRadius", "shapeKind", "wb_locked", "lockUniScaling"]);
  }

  function cloneJson(value) {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_) {
      return null;
    }
  }

  function nextActionId() {
    return crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
  }

  function buildAction(op, payload = {}) {
    localOpSeq += 1;
    const clientId = myClientId || historyClientId || "client";
    return {
      v: 1,
      op,
      client_id: clientId,
      seq: localOpSeq,
      ts: Date.now(),
      action_id: nextActionId(),
      surface_id: ACTIVE_SURFACE_ID,
      ...payload,
    };
  }

  function serializeAbsoluteObject(obj) {
    const json = serializeObject(obj);
    if (!obj || !fabric || !fabric.util || typeof obj.calcTransformMatrix !== "function") {
      return json;
    }

    try {
      const matrix = obj.calcTransformMatrix();
      const dec = fabric.util.qrDecompose(matrix);
      const center = typeof obj.getCenterPoint === "function" ? obj.getCenterPoint() : null;

      json.originX = "center";
      json.originY = "center";
      if (center) {
        json.left = center.x;
        json.top = center.y;
      } else {
        json.left = dec.translateX;
        json.top = dec.translateY;
      }
      if (Number.isFinite(dec.angle)) json.angle = dec.angle;
      if (Number.isFinite(dec.scaleX)) json.scaleX = dec.scaleX;
      if (Number.isFinite(dec.scaleY)) json.scaleY = dec.scaleY;
      if (Number.isFinite(dec.skewX)) json.skewX = dec.skewX;
      if (Number.isFinite(dec.skewY)) json.skewY = dec.skewY;
    } catch (_) {
      return json;
    }

    return json;
  }

  function isSyncableObject(obj) {
    if (!obj || typeof obj.toObject !== "function") return false;
    const t = String(obj.type || "").toLowerCase();
    return t !== "activeselection";
  }

  function captureActiveSelectionIds() {
    const active = fabricCanvas.getActiveObject();
    if (!active) return [];
    if (isActiveSelectionObject(active)) {
      return active.getObjects().map((o) => o && o.obj_id).filter(Boolean);
    }
    return active.obj_id ? [active.obj_id] : [];
  }

  function restoreSelectionByIds(objIds) {
    if (!Array.isArray(objIds) || !objIds.length) {
      updateRotateButtonState();
      return;
    }
    const byId = new Map();
    fabricCanvas.getObjects().forEach((o) => {
      if (o && o.obj_id) byId.set(o.obj_id, o);
    });
    const objects = objIds.map((id) => byId.get(id)).filter(Boolean);
    if (!objects.length) {
      updateRotateButtonState();
      return;
    }

    if (objects.length === 1) {
      fabricCanvas.setActiveObject(objects[0]);
    } else {
      const selection = new fabric.ActiveSelection(objects, { canvas: fabricCanvas });
      fabricCanvas.setActiveObject(selection);
    }
    applySelectionStyles();
  }

  function enqueueOps(ops) {
    // Note: deliberately does NOT gate on isRemoteApplying. That flag is only
    // true while a remote batch is being enlivened/applied (an async, awaited
    // process), and during that window the browser still delivers the local
    // user's own input events (drawing, typing, dragging). Gating here used to
    // silently drop those genuine local edits whenever they raced with someone
    // else's incoming update - the more concurrent editors, the more often it
    // happened. Echoing back a remote-applied object is prevented separately,
    // at the object:added/object:removed handlers below, which never call
    // into enqueueOps in the first place.
    if (!canEdit || suppressBroadcast) return;
    const normalized = (Array.isArray(ops) ? ops : [ops]).filter((op) => op && typeof op === "object");
    if (!normalized.length) return;
    pendingOps.push(...normalized);
    if (pendingOpsTimer) clearTimeout(pendingOpsTimer);
    pendingOpsTimer = setTimeout(() => {
      pendingOpsTimer = null;
      if (!pendingOps.length) return;
      const batch = pendingOps;
      pendingOps = [];
      socket.emit("batch_update", { ops: batch });
    }, 35);
  }

  function enqueueAddOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft) return;
    enqueueOps(buildAction("add", { object: serializeObject(obj) }));
  }

  function enqueueUpdateOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft) return;
    enqueueOps(buildAction("update", { object: serializeObject(obj) }));
  }

  function enqueueRemoveOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft || !obj.obj_id) return;
    enqueueOps(buildAction("remove", { obj_id: obj.obj_id, object_id: obj.obj_id }));
  }

  function enqueueSelectionUpdates() {
    const objects = getPanelSelectionObjects();
    if (!objects.length) return;
    enqueueOps(
      objects
        .filter((obj) => isSyncableObject(obj))
        .map((obj) => buildAction("update", { object: serializeObject(obj) })),
    );
  }

  function isEditingTextNow() {
    const active = fabricCanvas.getActiveObject();
    return !!(active && active.type === "i-text" && active.isEditing);
  }

  function syncITextLayout(target, keepFocus = false) {
    if (!target || target.type !== "i-text") return;
    target.dirty = true;
    if (typeof target.initDimensions === "function") target.initDimensions();
    target.setCoords();
    if (target.isEditing && typeof target._updateTextarea === "function") {
      target._updateTextarea();
      if (keepFocus && target.hiddenTextarea && typeof target.hiddenTextarea.focus === "function") {
        target.hiddenTextarea.focus({ preventScroll: true });
      }
    }
    fabricCanvas.requestRenderAll();
  }

  function scheduleITextLayoutSync(target, attempts = 4, keepFocus = false) {
    if (!target || target.type !== "i-text") return;
    let left = Math.max(1, Number(attempts) || 1);
    const step = () => {
      if (!target.canvas) return;
      syncITextLayout(target, keepFocus);
      left -= 1;
      if (left > 0) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  function syncAllITextLayouts() {
    for (const obj of fabricCanvas.getObjects()) {
      if (obj && obj.type === "i-text") syncITextLayout(obj);
    }
  }

  function copyActiveSelection() {
    let objects = getPanelSelectionObjects().filter((obj) => isSyncableObject(obj) && !obj._isDraft);
    if (!objects.length && focusedLockedObject && isObjectOnCanvas(focusedLockedObject) && isSyncableObject(focusedLockedObject)) {
      objects = [focusedLockedObject];
    }
    if (!objects.length) return false;
    copiedSelectionPayload = objects.map((obj) => serializeAbsoluteObject(obj));
    pasteShiftCount = 0;
    return true;
  }

  function estimateDataUrlBytes(dataUrl) {
    if (typeof dataUrl !== "string") return 0;
    const comma = dataUrl.indexOf(",");
    if (comma < 0) return 0;
    const base64 = dataUrl.slice(comma + 1);
    const padding = base64.endsWith("==") ? 2 : (base64.endsWith("=") ? 1 : 0);
    return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
  }

  function estimateJsonBytes(payload) {
    try {
      const json = JSON.stringify(payload);
      if (typeof json !== "string") return 0;
      if (jsonEncoder) return jsonEncoder.encode(json).length;
      return unescape(encodeURIComponent(json)).length;
    } catch (_) {
      return 0;
    }
  }

  function isImageFile(file) {
    return !!(file && typeof file.type === "string" && file.type.toLowerCase().startsWith("image/"));
  }

  function hasDraggedFiles(dataTransfer) {
    if (!dataTransfer) return false;
    if (dataTransfer.files && dataTransfer.files.length > 0) return true;
    const types = dataTransfer.types;
    if (!types) return false;
    if (typeof types.includes === "function") return types.includes("Files");
    return Array.from(types).includes("Files");
  }

  const MIRO_TOKEN_STORAGE_KEY = "wb_miro_token";

  function closeModal() {
    if (!modalOverlay) return;
    modalOverlay.classList.remove("active");
    if (confirmModal) confirmModal.classList.remove("active");
    if (miroModal) miroModal.classList.remove("active");
  }

  function openModalCard(card) {
    if (!modalOverlay || !card) return;
    [confirmModal, miroModal].forEach((c) => c && c.classList.remove("active"));
    card.classList.add("active");
    modalOverlay.classList.add("active");
  }

  if (modalOverlay) {
    modalOverlay.addEventListener("mousedown", (e) => {
      if (e.target === modalOverlay) closeModal();
    });
  }
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modalOverlay && modalOverlay.classList.contains("active")) closeModal();
  });

  // Generic yes/no dialog (currently only used for clearing the board, but
  // written to take a title/body/label so it isn't a one-off) - replaces
  // window.confirm with something that matches the app's own chrome instead
  // of an OS-styled browser dialog.
  function showConfirmModal({ title, body, confirmLabel = "Удалить" } = {}) {
    return new Promise((resolve) => {
      if (!modalOverlay || !confirmModal) {
        resolve(window.confirm(body || title || "Вы уверены?"));
        return;
      }
      if (confirmModalTitle) confirmModalTitle.textContent = title || "Подтвердите действие";
      if (confirmModalBody) confirmModalBody.textContent = body || "";
      if (confirmModalOk) confirmModalOk.textContent = confirmLabel;
      openModalCard(confirmModal);
      const cleanup = () => {
        confirmModalOk.removeEventListener("click", onOk);
        confirmModalCancel.removeEventListener("click", onCancel);
      };
      const onOk = () => {
        cleanup();
        closeModal();
        resolve(true);
      };
      const onCancel = () => {
        cleanup();
        closeModal();
        resolve(false);
      };
      confirmModalOk.addEventListener("click", onOk);
      confirmModalCancel.addEventListener("click", onCancel);
    });
  }

  // Accepts either a bare board ID or a full board URL (miro.com/app/board/
  // XXXX=/...) so users don't have to hunt through the URL to pull the ID
  // out themselves - just paste whatever's in the address bar.
  function extractMiroBoardId(raw) {
    const value = String(raw || "").trim();
    if (!value) return "";
    const match = value.match(/\/board\/([^/?#]+)/);
    if (match) return decodeURIComponent(match[1]);
    return value;
  }

  function setMiroModalStatus(message, type) {
    if (!miroModalStatus) return;
    miroModalStatus.textContent = message || "";
    miroModalStatus.className = `modal-status${type ? ` is-${type}` : ""}`;
  }

  function openMiroImportModal() {
    if (!modalOverlay || !miroModal) return;
    setMiroModalStatus("");
    if (miroBoardUrlInput) miroBoardUrlInput.value = "";
    // The token is remembered per-browser (never sent anywhere but this
    // board's own import request) so re-importing, or importing a second
    // board from the same Miro account, doesn't mean digging the token back
    // out of Miro's settings every time.
    const savedToken = localStorage.getItem(MIRO_TOKEN_STORAGE_KEY) || "";
    if (miroTokenInput) miroTokenInput.value = savedToken;
    if (miroRememberToken) miroRememberToken.checked = true;
    openModalCard(miroModal);
    requestAnimationFrame(() => miroBoardUrlInput && miroBoardUrlInput.focus());
  }

  function showBoardNotice(message, type = "error") {
    const text = String(message || "").trim();
    if (!text) return;
    if (!boardNoticeEl) return;
    boardNoticeEl.textContent = text;
    boardNoticeEl.classList.remove("is-success", "is-info", "is-warning");
    if (type !== "error") boardNoticeEl.classList.add(`is-${type}`);
    boardNoticeEl.classList.add("is-visible");
    if (boardNoticeTimer) clearTimeout(boardNoticeTimer);
    boardNoticeTimer = setTimeout(() => {
      boardNoticeEl.classList.remove("is-visible");
    }, 2400);
  }

  function showConnectionNotice(message, throttleMs = 2600, type = "warning") {
    const now = Date.now();
    if (throttleMs > 0 && now - lastConnectionNoticeAt < throttleMs) return;
    lastConnectionNoticeAt = now;
    showBoardNotice(message, type);
  }

  function readFileAsDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("image_read_failed"));
      reader.readAsDataURL(file);
    });
  }

  function loadImageElement(dataUrl) {
    return new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = () => reject(new Error("image_decode_failed"));
      img.src = dataUrl;
    });
  }

  function canvasToDataUrl(canvas, mime, quality) {
    try {
      return canvas.toDataURL(mime, quality);
    } catch (_) {
      return "";
    }
  }

  async function optimizeImageFile(file) {
    const original = await readFileAsDataUrl(file);
    const img = await loadImageElement(original);
    const srcW = Math.max(1, Number(img.naturalWidth || img.width || 1));
    const srcH = Math.max(1, Number(img.naturalHeight || img.height || 1));
    const sourceType = String(file?.type || "").toLowerCase();
    const sourceNeedsReencode = /image\/(png|bmp|tiff|gif|heic|heif)/i.test(sourceType);
    const scale = Math.min(1, MAX_IMAGE_IMPORT_SIDE / Math.max(srcW, srcH));
    let dstW = Math.max(1, Math.round(srcW * scale));
    let dstH = Math.max(1, Math.round(srcH * scale));

    const needResize = dstW !== srcW || dstH !== srcH;
    const originalBytes = estimateDataUrlBytes(original);
    if (!needResize && !sourceNeedsReencode && originalBytes <= TARGET_IMAGE_BYTES) return original;

    let best = original;
    let bestBytes = originalBytes || Number.MAX_SAFE_INTEGER;

    for (let attempt = 0; attempt < 4; attempt += 1) {
      const canvas = document.createElement("canvas");
      canvas.width = dstW;
      canvas.height = dstH;
      const ctx = canvas.getContext("2d");
      if (!ctx) break;
      ctx.drawImage(img, 0, 0, dstW, dstH);

      const candidates = [
        canvasToDataUrl(canvas, "image/webp", 0.86),
        canvasToDataUrl(canvas, "image/jpeg", 0.86),
        canvasToDataUrl(canvas, "image/webp", 0.78),
        canvasToDataUrl(canvas, "image/jpeg", 0.78),
        canvasToDataUrl(canvas, "image/webp", 0.68),
        canvasToDataUrl(canvas, "image/jpeg", 0.68),
        canvasToDataUrl(canvas, "image/png"),
      ].filter(Boolean);

      for (const candidate of candidates) {
        const bytes = estimateDataUrlBytes(candidate);
        if (bytes > 0 && bytes < bestBytes) {
          best = candidate;
          bestBytes = bytes;
        }
        if (bytes > 0 && bytes <= TARGET_IMAGE_BYTES) return candidate;
      }

      if (dstW <= 680 && dstH <= 680) break;
      dstW = Math.max(1, Math.round(dstW * 0.82));
      dstH = Math.max(1, Math.round(dstH * 0.82));
    }
    return best;
  }

  function willExceedBoardLimitWithObject(objectJson) {
    if (!objectJson || typeof objectJson !== "object") return false;
    const objects = fabricCanvas.getObjects()
      .filter((obj) => isSyncableObject(obj) && !obj._isDraft)
      .map((obj) => serializeObject(obj));
    objects.push(objectJson);
    const projectedCanvas = {
      version: "6.0.0",
      background: "#ffffff",
      objects,
    };
    const projectedSize = estimateJsonBytes(projectedCanvas);
    return projectedSize > 0 && projectedSize > BOARD_SOFT_LIMIT_BYTES;
  }

  function normalizeBoardErrorMessage(message) {
    const text = String(message || "").trim();
    if (!text) return "Ошибка синхронизации доски";
    if (/unsupported_file_type|unsupported format/i.test(text)) {
      return "Неподдерживаемый формат файла";
    }
    if (/image_read_failed|image_decode_failed/i.test(text)) {
      return "Не удалось прочитать изображение";
    }
    if (/15\s*MB|size exceeds|payload|too large/i.test(text)) {
      return "Изображение слишком большое для синхронизации. Уменьшите размер и попробуйте снова.";
    }
    return text;
  }

  function showBoardError(message) {
    const text = normalizeBoardErrorMessage(message);
    console.error(String(message || text));
    showBoardNotice(text);
  }

  function cloneForPaste(jsonObject) {
    return Promise.resolve(fabric.util.enlivenObjects([jsonObject]))
      .then((list) => (Array.isArray(list) && list.length ? list[0] : null))
      .catch(() => null);
  }

  async function pasteCopiedSelection() {
    if (!canEdit || !copiedSelectionPayload.length) return;
    const payload = cloneJson(copiedSelectionPayload) || [];
    if (!payload.length) return;

    pasteShiftCount += 1;
    const dx = PASTE_SHIFT_STEP * pasteShiftCount;
    const dy = PASTE_SHIFT_STEP * pasteShiftCount;

    const inserted = [];
    for (const item of payload) {
      if (!item || typeof item !== "object") continue;
      const cloned = await cloneForPaste(item);
      if (!cloned) continue;
      cloned.set({
        left: Number(cloned.left || 0) + dx,
        top: Number(cloned.top || 0) + dy,
      });
      cloned.obj_id = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
      cloned.author_id = myUserId || cloned.author_id || "unknown";
      cloned.author_name = localUsername || cloned.author_name || "user";
      setObjectLocked(cloned, !!cloned.wb_locked);
      enforceNoMirrorObject(cloned);
      cloned.setCoords();
      fabricCanvas.add(cloned);
      inserted.push(cloned);
    }

    if (!inserted.length) return;

    const ops = inserted.map((obj) => buildAction("add", { object: serializeObject(obj) }));
    enqueueOps(ops);

    if (inserted.length === 1) {
      fabricCanvas.setActiveObject(inserted[0]);
    } else {
      const selection = new fabric.ActiveSelection(inserted, { canvas: fabricCanvas });
      fabricCanvas.setActiveObject(selection);
    }
    applySelectionStyles();
    updateStylePanelVisibility();
    fabricCanvas.requestRenderAll();
  }

  function enqueueTextUpdateDebounced(obj) {
    if (!obj) return;
    if (pendingTextSyncTimer) clearTimeout(pendingTextSyncTimer);
    pendingTextSyncTimer = setTimeout(() => {
      pendingTextSyncTimer = null;
      enqueueUpdateOp(obj);
    }, 60);
  }

  function withTimeout(promise, timeoutMs, timeoutMessage = "timeout") {
    const t = Number(timeoutMs || 0);
    if (!Number.isFinite(t) || t <= 0) return promise;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(timeoutMessage)), t);
      Promise.resolve(promise)
        .then((value) => {
          clearTimeout(timer);
          resolve(value);
        })
        .catch((err) => {
          clearTimeout(timer);
          reject(err);
        });
    });
  }

  function applyCanvasState(canvasJson) {
    const selectedIds = captureActiveSelectionIds();
    isRemoteApplying = true;
    withTimeout(
      Promise.resolve(fabricCanvas.loadFromJSON(canvasJson)),
      LOAD_FROM_JSON_TIMEOUT_MS,
      "load_from_json_timeout",
    ).then(() => {
      isRemoteApplying = false;
      ensureCanvasTransparentBackground();
      syncLockStates();
      syncRoundedImages();
      syncNoMirrorObjects();
      syncAllITextLayouts();
      syncObjectInteractivity();
      restoreSelectionByIds(selectedIds);
      updateStylePanelVisibility();
      updateRotateButtonState();
      drawMiniMap();
    }).catch((err) => {
      console.warn("Failed to apply full canvas state, falling back to safe empty state", err);
      showBoardNotice("Не удалось загрузить часть содержимого доски");
      try {
        fabricCanvas.clear();
        ensureCanvasTransparentBackground();
        syncObjectInteractivity();
        updateStylePanelVisibility();
        updateRotateButtonState();
        drawMiniMap();
      } catch (_) {
        // no-op
      }
      isRemoteApplying = false;
    });
  }

  function findObjectById(objId) {
    if (!objId) return null;
    const objects = fabricCanvas.getObjects();
    for (const obj of objects) {
      if (obj && obj.obj_id === objId) return obj;
    }
    return null;
  }

  async function enlivenOne(objectJson) {
    if (!objectJson || typeof objectJson !== "object") return null;
    const list = await fabric.util.enlivenObjects([objectJson]);
    return Array.isArray(list) && list.length ? list[0] : null;
  }

  async function applyRemoteOp(op) {
    if (!op || typeof op !== "object") return;
    const type = String(op.type || op.op || "").toLowerCase();
    if (type === "remove") {
      const target = findObjectById(op.obj_id || op.object_id);
      if (target) fabricCanvas.remove(target);
      return;
    }
    if (type === "add" || type === "update") {
      const objectJson = op.object;
      if (!objectJson || typeof objectJson !== "object") return;
      let enlivened = null;
      try {
        enlivened = await enlivenOne(objectJson);
      } catch (err) {
        console.error("[board] failed to enliven remote object", objectJson && objectJson.type, err);
        enlivened = null;
      }
      if (!enlivened) return;
      ensureObjMeta(enlivened);
      setObjectLocked(enlivened, !!objectJson.wb_locked);
      enforceNoMirrorObject(enlivened);
      const existing = findObjectById(objectJson.obj_id || objectJson.object_id);
      if (existing) {
        fabricCanvas.remove(existing);
      }
      fabricCanvas.add(enlivened);
      if (type === "update") {
        const activeIds = new Set(captureActiveSelectionIds());
        if (activeIds.has(objectJson.obj_id || objectJson.object_id)) {
          restoreSelectionByIds([...activeIds]);
        }
      }
    }
  }

  function applyRemoteOps(ops) {
    const list = Array.isArray(ops) ? ops : [];
    if (!list.length) return;
    const selectedIds = captureActiveSelectionIds();
    remoteOpsChain = remoteOpsChain.then(async () => {
      isRemoteApplying = true;
      try {
        for (const op of list) {
          try {
            await applyRemoteOp(op);
          } catch (err) {
            // Skip bad op and continue applying the rest, but surface it -
            // silently swallowing this made real sync bugs invisible.
            console.error("[board] failed to apply remote op", op, err);
          }
        }
      } finally {
        isRemoteApplying = false;
      }
      syncRoundedImages();
      syncLockStates();
      syncAllITextLayouts();
      syncObjectInteractivity();
      restoreSelectionByIds(selectedIds);
      updateStylePanelVisibility();
      updateRotateButtonState();
      fabricCanvas.requestRenderAll();
      drawMiniMap();
    }).catch(() => {
      isRemoteApplying = false;
    });
  }

  function ensureRemoteCursor(clientId, username) {
    if (remoteCursors.has(clientId)) return remoteCursors.get(clientId);
    const hash = [...clientId].reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
    const color = cursorColors[hash % cursorColors.length];
    const el = document.createElement("div");
    el.className = "remote-cursor";
    el.style.color = color;
    el.innerHTML = `<span class="bi-local" style="--icon:url('/static/icons/cursor.svg')"></span><span class="name">${username || "user"}</span>`;
    const nameEl = el.querySelector(".name");
    if (nameEl) {
      nameEl.style.borderColor = `${color}66`;
      nameEl.style.background = `${color}1a`;
      nameEl.style.color = color;
    }
    cursorLayer.appendChild(el);
    const item = { el, x: 0, y: 0, tx: 0, ty: 0, ts: Date.now(), color };
    remoteCursors.set(clientId, item);
    return item;
  }

  function removeRemoteCursor(clientId) {
    const item = remoteCursors.get(clientId);
    if (!item) return;
    item.el.remove();
    remoteCursors.delete(clientId);
  }

  function renderRemoteCursors() {
    const w = fabricCanvas.getWidth();
    const h = fabricCanvas.getHeight();
    const pad = 10;
    for (const item of remoteCursors.values()) {
      item.x += (item.tx - item.x) * 0.35;
      item.y += (item.ty - item.y) * 0.35;
      const p = screenFromWorld(item.x, item.y);
      item.el.style.display = "block";

      const clampedX = Math.max(pad, Math.min(w - pad, p.x));
      const clampedY = Math.max(pad, Math.min(h - pad, p.y));
      const offscreen = p.x < 0 || p.y < 0 || p.x > w || p.y > h;

      item.el.classList.toggle("offscreen", offscreen);
      item.el.style.left = `${clampedX}px`;
      item.el.style.top = `${clampedY}px`;
    }
  }

  function ensureCursorAnimation() {
    if (cursorAnimFrame) return;
    const tick = () => {
      renderRemoteCursors();
      cursorAnimFrame = requestAnimationFrame(tick);
    };
    cursorAnimFrame = requestAnimationFrame(tick);
  }

  function drawMiniMap() {
    const w = miniMapEl.width;
    const h = miniMapEl.height;

    miniCtx.clearRect(0, 0, w, h);
    miniCtx.fillStyle = "#f3faf5";
    miniCtx.fillRect(0, 0, w, h);

    const objectBounds = [];
    for (const obj of fabricCanvas.getObjects()) {
      if (!obj || typeof obj.getBoundingRect !== "function") continue;
      const b = obj.getBoundingRect();
      if (!b || !Number.isFinite(b.left) || !Number.isFinite(b.top) || !Number.isFinite(b.width) || !Number.isFinite(b.height)) {
        continue;
      }
      objectBounds.push({ minX: b.left, minY: b.top, maxX: b.left + b.width, maxY: b.top + b.height });
    }

    const c1 = worldFromScreen(0, 0);
    const c2 = worldFromScreen(fabricCanvas.getWidth(), fabricCanvas.getHeight());
    const vp = {
      minX: Math.min(c1.x, c2.x),
      minY: Math.min(c1.y, c2.y),
      maxX: Math.max(c1.x, c2.x),
      maxY: Math.max(c1.y, c2.y),
    };

    let minX = vp.minX;
    let minY = vp.minY;
    let maxX = vp.maxX;
    let maxY = vp.maxY;

    for (const b of objectBounds) {
      minX = Math.min(minX, b.minX);
      minY = Math.min(minY, b.minY);
      maxX = Math.max(maxX, b.maxX);
      maxY = Math.max(maxY, b.maxY);
    }

    const pad = 40;
    minX -= pad;
    minY -= pad;
    maxX += pad;
    maxY += pad;

    const worldW = Math.max(1, maxX - minX);
    const worldH = Math.max(1, maxY - minY);
    const sx = w / worldW;
    const sy = h / worldH;
    const scale = Math.min(sx, sy);
    const ox = (w - worldW * scale) / 2;
    const oy = (h - worldH * scale) / 2;

    const toMini = (x, y) => ({ x: ox + (x - minX) * scale, y: oy + (y - minY) * scale });

    // Darker green for content blobs, lighter green fill for the viewport
    // rectangle below - both green, but distinct enough that the viewport
    // still reads as the highlighted "you are here" element, Miro-style.
    miniCtx.fillStyle = "rgba(21,128,61,0.45)";
    for (const b of objectBounds) {
      const p1 = toMini(b.minX, b.minY);
      const p2 = toMini(b.maxX, b.maxY);
      miniCtx.fillRect(p1.x, p1.y, Math.max(2, p2.x - p1.x), Math.max(2, p2.y - p1.y));
    }

    const v1 = toMini(vp.minX, vp.minY);
    const v2 = toMini(vp.maxX, vp.maxY);
    const vpX = v1.x;
    const vpY = v1.y;
    const vpW = Math.max(4, v2.x - v1.x);
    const vpH = Math.max(4, v2.y - v1.y);

    miniCtx.fillStyle = "rgba(22,163,74,0.14)";
    miniCtx.fillRect(vpX, vpY, vpW, vpH);
    miniCtx.strokeStyle = "#16a34a";
    miniCtx.lineWidth = 1.4;
    miniCtx.strokeRect(vpX, vpY, vpW, vpH);

    miniState = { minX, minY, scale, ox, oy, vpRect: { x: vpX, y: vpY, w: vpW, h: vpH } };
  }

  function miniToWorld(px, py) {
    const x = (px - miniState.ox) / Math.max(miniState.scale, 1e-8) + miniState.minX;
    const y = (py - miniState.oy) / Math.max(miniState.scale, 1e-8) + miniState.minY;
    return { x, y };
  }

  function handleMiniPointer(clientX, clientY) {
    const rect = miniMapEl.getBoundingClientRect();
    const x = Math.max(0, Math.min(rect.width, clientX - rect.left));
    const y = Math.max(0, Math.min(rect.height, clientY - rect.top));
    const w = miniToWorld((x / rect.width) * miniMapEl.width, (y / rect.height) * miniMapEl.height);
    centerOnWorld(w.x, w.y);
  }

  function activeDropWorldPoint(clientX, clientY) {
    const rect = boardWrap.getBoundingClientRect();
    return worldFromScreen(clientX - rect.left, clientY - rect.top);
  }

  async function dataUrlToBlob(dataUrl) {
    const res = await fetch(dataUrl);
    return res.blob();
  }

  async function uploadImageToServer(blobOrFile, filename) {
    const form = new FormData();
    form.append("file", blobOrFile, filename || "image");
    const headers = {};
    if (pageToken) headers["Authorization"] = `Bearer ${pageToken}`;
    const res = await fetch(`/api/board/${encodeURIComponent(boardId)}/upload-image`, {
      method: "POST",
      headers,
      body: form,
    });
    if (!res.ok) {
      let detail = "";
      try {
        detail = (await res.json()).detail || "";
      } catch (_) {
        // ignore, fall back to generic message below
      }
      throw new Error(detail || "image_upload_failed");
    }
    // The endpoint now returns immediately with a job_id (see main.py's
    // upload_image) - the actual compression happens on a Celery worker,
    // polled for via pollImageUploadJob below.
    return res.json();
  }

  async function pollImageUploadJob(jobId, { intervalMs = 700, timeoutMs = 60000 } = {}) {
    const headers = {};
    if (pageToken) headers["Authorization"] = `Bearer ${pageToken}`;
    const startedAt = Date.now();
    while (Date.now() - startedAt < timeoutMs) {
      const res = await fetch(
        `/api/board/${encodeURIComponent(boardId)}/upload-image/${encodeURIComponent(jobId)}`,
        { headers },
      );
      if (!res.ok) throw new Error("image_status_failed");
      const data = await res.json();
      if (data.status === "done") return data;
      if (data.status === "error") throw new Error(data.detail || "image_processing_failed");
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
    throw new Error("image_processing_timeout");
  }

  // Loops a slow opacity pulse on the placeholder rect so "an image is on
  // its way" reads clearly while the Celery worker compresses it, instead of
  // the canvas just sitting there with nothing happening for however long
  // that takes on a large photo.
  function pulseImagePlaceholder(rect) {
    if (!rect || !isObjectOnCanvas(rect) || !rect.wb_isImagePlaceholder) return;
    fabric.util.animate({
      startValue: 0.35,
      endValue: 0.75,
      duration: 550,
      onChange: (v) => {
        rect.set({ opacity: v });
        fabricCanvas.requestRenderAll();
      },
      onComplete: () => {
        if (!isObjectOnCanvas(rect) || !rect.wb_isImagePlaceholder) return;
        fabric.util.animate({
          startValue: 0.75,
          endValue: 0.35,
          duration: 550,
          onChange: (v) => {
            rect.set({ opacity: v });
            fabricCanvas.requestRenderAll();
          },
          onComplete: () => pulseImagePlaceholder(rect),
        });
      },
    });
  }

  function createImageUploadPlaceholder(worldX, worldY, naturalWidth, naturalHeight) {
    const maxW = 320;
    const ratio = naturalWidth && naturalHeight ? naturalHeight / naturalWidth : 0.72;
    const w = naturalWidth ? Math.min(maxW, naturalWidth) : 220;
    const h = Math.round(w * ratio);
    const rect = new fabric.Rect({
      left: worldX - w / 2,
      top: worldY - h / 2,
      width: w,
      height: h,
      rx: 12,
      ry: 12,
      fill: "rgba(22,163,74,0.10)",
      stroke: "rgba(22,163,74,0.35)",
      strokeDashArray: [7, 5],
      strokeWidth: 1.5,
      selectable: false,
      evented: false,
      excludeFromExport: true,
      opacity: 0.35,
    });
    rect.wb_isImagePlaceholder = true;
    fabricCanvas.add(rect);
    fabricCanvas.requestRenderAll();
    pulseImagePlaceholder(rect);
    return rect;
  }

  function removeImageUploadPlaceholder(rect) {
    if (!rect) return;
    rect.wb_isImagePlaceholder = false;
    if (isObjectOnCanvas(rect)) fabricCanvas.remove(rect);
  }

  // Reads just the pixel dimensions of the picked file, client-side, so the
  // loading placeholder can be sized to roughly the final image's aspect
  // ratio instead of a generic box that then jumps/resizes once the real
  // image comes back.
  function readImageNaturalSize(file) {
    return new Promise((resolve) => {
      const url = URL.createObjectURL(file);
      const img = new Image();
      img.onload = () => {
        URL.revokeObjectURL(url);
        resolve({ width: img.naturalWidth, height: img.naturalHeight });
      };
      img.onerror = () => {
        URL.revokeObjectURL(url);
        resolve({ width: 0, height: 0 });
      };
      img.src = url;
    });
  }

  function addImageAtWorldPoint(dataUrl, worldX, worldY) {
    return fabric.Image.fromURL(dataUrl, { crossOrigin: "anonymous" }).then((img) => {
      const maxW = 640;
      const scale = img.width > maxW ? maxW / img.width : 1;
      img.scale(scale);
      img.set({
        left: worldX - img.getScaledWidth() / 2,
        top: worldY - img.getScaledHeight() / 2,
        lockUniScaling: true,
        lockScalingFlip: true,
        flipX: false,
        flipY: false,
      });
      ensureObjMeta(img);
      const serialized = serializeObject(img);
      if (willExceedBoardLimitWithObject(serialized)) {
        showBoardError("Изображение слишком большое для текущей доски");
        return;
      }
      fabricCanvas.add(img);
      enqueueOps(buildAction("add", { object: serialized }));
      fabricCanvas.setActiveObject(img);
      setTool("select");
      updateStylePanelVisibility();
    }).catch((err) => {
      showBoardError(err?.message || "Не удалось добавить изображение");
    });
  }

  async function addImageFileAtWorldPoint(file, worldX, worldY) {
    if (!isImageFile(file)) throw new Error("unsupported_file_type");

    const naturalSize = await readImageNaturalSize(file);
    const placeholder = createImageUploadPlaceholder(worldX, worldY, naturalSize.width, naturalSize.height);

    try {
      // Pre-shrink client-side first: cuts upload time/bandwidth and gives
      // the backend an already-small image to re-encode (the backend still
      // re-encodes to WEBP and enforces the hard size cap - see
      // app/image_processing.py - this is just an optimization, not what
      // makes the upload non-blocking; that's the job_id/poll flow below).
      let dataUrl = "";
      try {
        dataUrl = await optimizeImageFile(file);
      } catch (_) {
        dataUrl = "";
      }
      const uploadBlob = dataUrl ? await dataUrlToBlob(dataUrl) : file;
      const uploaded = await uploadImageToServer(uploadBlob, file.name);
      const result = await pollImageUploadJob(uploaded.job_id);
      removeImageUploadPlaceholder(placeholder);
      await addImageAtWorldPoint(result.url, worldX, worldY);
    } catch (err) {
      removeImageUploadPlaceholder(placeholder);
      showBoardError(err?.message || "Не удалось добавить изображение");
    }
  }

  function worldCenterOfViewport() {
    return worldFromScreen(fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2);
  }

  function deleteActiveObjects() {
    // Bug fix: the old check here treated ANY object with a getObjects()
    // method as a multi-selection, including a single grouped object (arrow,
    // sticker) - it would then try to canvas.remove() the group's own
    // children, which are not top-level canvas objects, so nothing actually
    // happened. Deleting a lone arrow/sticker via Delete/Backspace or this
    // button silently did nothing before this fix. Using
    // resolveSelectionAnchor() also covers deleting a locked-but-focused
    // object, which has no true Fabric selection to read.
    const { objects } = resolveSelectionAnchor();
    if (!objects.length) return;
    fabricCanvas.discardActiveObject();
    objects.forEach((o) => fabricCanvas.remove(o));
    if (focusedLockedObject && objects.includes(focusedLockedObject)) focusedLockedObject = null;
    fabricCanvas.requestRenderAll();
    updateSelectionToolbar();
  }

  function createStickyNote(left, top, width, height, color, text, fontSize) {
    const note = new StickyNote({
      left,
      top,
      width,
      height,
      fill: color || currentStickerColor,
      noteText: text || "",
      noteFontSize: fontSize || STICKER_DEFAULT_FONT_SIZE,
      originX: "left",
      originY: "top",
    });
    ensureObjMeta(note);
    return note;
  }

  function createShape(type, start, end) {
    const x = Math.min(start.x, end.x);
    const y = Math.min(start.y, end.y);
    const width = Math.max(1, Math.abs(end.x - start.x));
    const height = Math.max(1, Math.abs(end.y - start.y));
    const strokeW = Number(strokeWidthEl.value);

    if (type === "sticker") {
      const dragW = Math.abs(end.x - start.x);
      const dragH = Math.abs(end.y - start.y);
      let w;
      let h;
      let left;
      let top;
      if (dragW < 20 && dragH < 20) {
        // Plain click (no meaningful drag): drop a default-size note centered
        // on the click point, like addImageAtWorldPoint does for images.
        w = STICKER_DEFAULT_W;
        h = STICKER_DEFAULT_H;
        left = start.x - w / 2;
        top = start.y - h / 2;
      } else {
        w = Math.max(STICKER_MIN_SIZE, dragW);
        h = Math.max(STICKER_MIN_SIZE, dragH);
        left = x;
        top = y;
      }
      return createStickyNote(left, top, w, h);
    }

    if (type === "rect") {
      const rect = new fabric.Rect({
        left: x,
        top: y,
        width,
        height,
        rx: 0,
        ry: 0,
        cornerRadius: 0,
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
      });
      ensureObjMeta(rect);
      return rect;
    }

    if (type === "ellipse") {
      const ell = new fabric.Ellipse({
        left: x,
        top: y,
        rx: width / 2,
        ry: height / 2,
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
        originX: "left",
        originY: "top",
      });
      ensureObjMeta(ell);
      return ell;
    }

    if (type === "line") {
      const line = new fabric.Line([start.x, start.y, end.x, end.y], {
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
      });
      ensureObjMeta(line);
      return line;
    }

    if (type === "arrow") {
      const x1 = start.x;
      const y1 = start.y;
      const x2 = end.x;
      const y2 = end.y;
      const angle = Math.atan2(y2 - y1, x2 - x1);
      const head = Math.max(10, strokeW * 3.5);
      const wing = Math.max(6, strokeW * 2.4);

      const line = new fabric.Line([x1, y1, x2, y2], {
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
      });

      const left = {
        x: x2 - head * Math.cos(angle) + wing * Math.sin(angle),
        y: y2 - head * Math.sin(angle) - wing * Math.cos(angle),
      };
      const right = {
        x: x2 - head * Math.cos(angle) - wing * Math.sin(angle),
        y: y2 - head * Math.sin(angle) + wing * Math.cos(angle),
      };
      const headTriangle = new fabric.Polygon(
        [
          { x: x2, y: y2 },
          { x: left.x, y: left.y },
          { x: right.x, y: right.y },
        ],
        {
          fill: currentColor,
          stroke: currentColor,
          strokeWidth: Math.max(1, strokeW * 0.8),
          strokeLineCap: "round",
          strokeLineJoin: "round",
        },
      );
      const group = new fabric.Group([line, headTriangle], {
        subTargetCheck: false,
      });
      ensureObjMeta(group);
      return group;
    }

    if (type === "triangle") {
      const tri = new fabric.Triangle({
        left: x,
        top: y,
        width,
        height,
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
        originX: "left",
        originY: "top",
      });
      ensureObjMeta(tri);
      return tri;
    }

    const diamond = new fabric.Polygon(
      [
        { x: width / 2, y: 0 },
        { x: width, y: height / 2 },
        { x: width / 2, y: height },
        { x: 0, y: height / 2 },
      ],
      {
        left: x,
        top: y,
        fill: "transparent",
        stroke: currentColor,
        strokeWidth: strokeW,
        strokeLineCap: "round",
        strokeLineJoin: "round",
        originX: "left",
        originY: "top",
      },
    );
    ensureObjMeta(diamond);
    return diamond;
  }

  function eraseAtEvent(evt) {
    const target = fabricCanvas.findTarget(evt);
    if (!target) return;
    fabricCanvas.remove(target);
  }

  function onMouseDown(opt) {
    const evt = opt.e;
    if (isTouchInputSuppressed(evt)) {
      if (typeof evt.preventDefault === "function") evt.preventDefault();
      if (typeof evt.stopPropagation === "function") evt.stopPropagation();
      return;
    }
    const target = opt && opt.target;
    const point = getClientPoint(evt);
    const isTouchInput = isTouchLikeEvent(evt);
    const leftLikeButton = evt.button === 0 || (isTouchInput && evt.button !== 1 && evt.button !== 2);
    const panByModifier = evt.button === 1 || (leftLikeButton && (evt.ctrlKey || evt.metaKey));
    const panByTool = currentTool === "hand" && leftLikeButton;

    if (panByModifier || panByTool) {
      if (typeof evt.preventDefault === "function") evt.preventDefault();
      if (typeof evt.stopPropagation === "function") evt.stopPropagation();
      panMode = true;
      if (point) panLast = { x: point.x, y: point.y };
      fabricCanvas.defaultCursor = "grabbing";
      return;
    }

    if (!canEdit) return;

    if (currentTool === "select" && target && isObjectLocked(target) && isLockEligibleObject(target)) {
      focusedLockedObject = target;
      updateSelectionToolbar();
      return;
    }

    focusedLockedObject = null;

    if (currentTool === "pencil" || currentTool === "shape" || currentTool === "text" || currentTool === "eraser") {
      hidePanels();
    }

    const p = fabricCanvas.getScenePoint(evt);

    if (currentTool === "text") {
      if (target && isTextModeSelectableObject(target)) {
        skipNextTextCreate = false;
        textEditArmedObjId = "";
        if (fabricCanvas.getActiveObject() !== target) {
          fabricCanvas.setActiveObject(target);
        }
        applySelectionStyles();
        updateStylePanelVisibility();
        updateSelectionToolbar();
        fabricCanvas.requestRenderAll();
        return;
      }

      const active = fabricCanvas.getActiveObject();
      if (active && isTextObject(active) && active.isEditing) return;
      if (skipNextTextCreate) {
        skipNextTextCreate = false;
        return;
      }
      textEditArmedObjId = "";

      const text = new fabric.IText("", {
        left: p.x,
        top: p.y,
        fill: currentColor,
        fontSize: Number(textSizeEl ? textSizeEl.value : 24),
        fontFamily: "Montserrat, sans-serif",
        fontWeight: "500",
        lockScalingFlip: true,
        lockUniScaling: true,
        flipX: false,
        flipY: false,
      });
      ensureObjMeta(text);
      fabricCanvas.add(text);
      enqueueAddOp(text);
      fabricCanvas.setActiveObject(text);
      syncITextLayout(text);
      text.enterEditing();
      scheduleITextLayoutSync(text, 5, true);
      return;
    }

    if (currentTool === "eraser") {
      erasing = true;
      eraseAtEvent(evt);
      return;
    }

    if (currentTool === "shape") {
      drawingStart = p;
      drawingShape = createShape(currentShapeType, p, p);
      drawingShape._isDraft = true;
      fabricCanvas.add(drawingShape);
      return;
    }
  }

  function onMouseMove(opt) {
    const evt = opt.e;
    if (isTouchInputSuppressed(evt)) {
      if (typeof evt.preventDefault === "function") evt.preventDefault();
      return;
    }
    const client = getClientPoint(evt);
    const rect = boardWrap.getBoundingClientRect();
    const offsetX = Number.isFinite(evt.offsetX) ? evt.offsetX : (client ? client.x - rect.left : 0);
    const offsetY = Number.isFinite(evt.offsetY) ? evt.offsetY : (client ? client.y - rect.top : 0);
    const world = worldFromScreen(offsetX, offsetY);

    if (panMode) {
      if (typeof evt.preventDefault === "function") evt.preventDefault();
      const v = fabricCanvas.viewportTransform;
      const cur = client || panLast;
      const dx = cur.x - panLast.x;
      const dy = cur.y - panLast.y;
      v[4] += dx;
      v[5] += dy;
      const active = fabricCanvas.getActiveObject();
      if (active && typeof active.setCoords === "function") active.setCoords();
      panLast = { x: cur.x, y: cur.y };
      fabricCanvas.requestRenderAll();
      renderRemoteCursors();
      drawMiniMap();
      updateGridPosition();
      updateSelectionToolbar();
    }

    if (erasing && currentTool === "eraser") {
      eraseAtEvent(evt);
    }

    if (drawingShape && drawingStart) {
      const p = fabricCanvas.getScenePoint(evt);
      fabricCanvas.remove(drawingShape);
      drawingShape = createShape(currentShapeType, drawingStart, p);
      drawingShape._isDraft = true;
      fabricCanvas.add(drawingShape);
      fabricCanvas.requestRenderAll();
    }

    const now = Date.now();
    if (now - lastCursorSentAt > 50) {
      socket.emit("cursor", { x: world.x, y: world.y });
      lastCursorSentAt = now;
    }
  }

  function onMouseUp() {
    if (pinchMode || Date.now() < suppressTouchInputUntil) return;
    if (panMode) {
      panMode = false;
      setTool(currentTool);
      return;
    }

    if (erasing) {
      erasing = false;
      return;
    }

    if (drawingShape) {
      drawingShape._isDraft = false;
      ensureObjMeta(drawingShape);
      enqueueAddOp(drawingShape);
      const createdSticker = isStickerObject(drawingShape) ? drawingShape : null;
      drawingShape = null;
      drawingStart = null;
      if (createdSticker) {
        // Jump straight into text editing, same convenience as the text tool.
        setTool("select");
        enterStickerEditMode(createdSticker);
      }
    }

  }

  fabricCanvas.on("mouse:down", onMouseDown);
  fabricCanvas.on("mouse:move", onMouseMove);
  fabricCanvas.on("mouse:up", onMouseUp);

  fabricCanvas.on("mouse:wheel", (opt) => {
    const evt = opt.e;
    const isZoomGesture = evt.ctrlKey || evt.metaKey;
    if (isZoomGesture) {
      const factor = evt.deltaY > 0 ? 0.94 : 1.065;
      zoomByFactor(factor, evt.offsetX, evt.offsetY);
    } else {
      const v = fabricCanvas.viewportTransform;
      v[4] -= evt.deltaX;
      v[5] -= evt.deltaY;
      const active = fabricCanvas.getActiveObject();
      if (active && typeof active.setCoords === "function") active.setCoords();
      fabricCanvas.requestRenderAll();
      renderRemoteCursors();
      drawMiniMap();
      updateGridPosition();
      updateSelectionToolbar();
    }
    opt.e.preventDefault();
    opt.e.stopPropagation();
  });

  fabricCanvas.on("text:editing:exited", (e) => {
    skipNextTextCreate = true;
    const target = e && e.target;
    if (!target || target.type !== "i-text") return;
    const textValue = String(target.text || "").trim();
    if (!textValue) {
      fabricCanvas.remove(target);
      return;
    }
    enqueueUpdateOp(target);
  });

  fabricCanvas.on("text:editing:entered", (e) => {
    const target = e && e.target;
    if (target && target.type === "i-text") scheduleITextLayoutSync(target, 5, true);
  });

  fabricCanvas.on("text:changed", (e) => {
    const target = e && e.target;
    if (target && target.type === "i-text") {
      syncITextLayout(target, true);
      enqueueTextUpdateDebounced(target);
    }
  });

  fabricCanvas.on("path:created", (e) => {
    if (pinchMode || Date.now() < suppressTouchInputUntil || Date.now() < suppressPathCreatedUntil) {
      if (e.path) fabricCanvas.remove(e.path);
      return;
    }
    if (e.path) {
      ensureObjMeta(e.path);
      e.path.set({
        stroke: currentColor,
        strokeWidth: Number(strokeWidthEl.value),
        strokeLineCap: "round",
        strokeLineJoin: "round",
        strokeMiterLimit: 2,
        opacity: 0.96,
      });
      enqueueAddOp(e.path);
    }
  });

  fabricCanvas.on("object:added", (e) => {
    if (isRemoteApplying) return;
    if (e.target) {
      ensureObjMeta(e.target);
      enforceNoMirrorInTarget(e.target);
    }
    drawMiniMap();
  });

  fabricCanvas.on("object:scaling", (e) => {
    const target = e && e.target;
    if (!target) return;
    let changed = enforceNoMirrorInTarget(target);
    // The canvas is set up with uniformScaling=false (so plain shapes can be
    // freely stretched into non-square rectangles by dragging a corner
    // without holding Shift) - but that canvas-level setting overrides a
    // sticker's own lockUniScaling:true for corner drags, which otherwise
    // should force a Group's scaleX/scaleY to move together. Rather than a
    // global canvas.uniformScaling=true (which would break free-resizing for
    // every other shape), just re-sync the two axes on every scaling tick
    // for stickers specifically - whichever axis moved further from 1 wins,
    // so a diagonal corner drag reads as "resize the note" instead of
    // stretching/squishing its text.
    if (isStickerObject(target) && target.scaleX !== target.scaleY) {
      const primary = Math.abs(target.scaleX - 1) >= Math.abs(target.scaleY - 1) ? target.scaleX : target.scaleY;
      target.set({ scaleX: primary, scaleY: primary });
      changed = true;
    }
    if (changed) {
      fabricCanvas.requestRenderAll();
    }
  });

  fabricCanvas.on("object:modified", (e) => {
    const target = e && e.target;
    if (target) {
      enforceNoMirrorInTarget(target);
      const applyOne = (obj) => {
        if (obj && obj.type === "image") applyImageCornerRadius(obj, getObjectCornerRadius(obj));
      };
      if (isActiveSelectionObject(target)) {
        // Was one socket.emit per object (emitUpdateOpImmediate in a loop) -
        // dragging a 20-object selection meant 20 separate WebSocket frames
        // and 20 separate server-side history/DB writes for what is, from
        // the user's perspective, a single move. Collect them into one
        // batch_update instead, same as every other multi-op path already does.
        // isActiveSelectionObject (not the old isMultiSelectionObject, which
        // matched ANY object with getObjects() - including a single sticker
        // or arrow group) - that false positive fed a dragged/resized/
        // rotated sticker's rect+textbox children into this branch as
        // independent top-level update ops instead of the group itself,
        // syncing them to every client as two loose objects sitting on top
        // of the real sticker.
        const ops = [];
        target.getObjects().forEach((obj) => {
          if (!obj) return;
          applyOne(obj);
          obj.setCoords();
          if (!isSyncableObject(obj) || obj._isDraft) return;
          ops.push(buildAction("update", { object: serializeAbsoluteObject(obj) }));
        });
        if (ops.length && canEdit && !suppressBroadcast) {
          socket.emit("batch_update", { ops });
        }
      } else {
        applyOne(target);
        enqueueUpdateOp(target);
      }
    }
    drawMiniMap();
    updateStylePanelVisibility();
  });

  fabricCanvas.on("object:removed", (e) => {
    if (!isRemoteApplying && e && e.target) enqueueRemoveOp(e.target);
    drawMiniMap();
    updateStylePanelVisibility();
  });

  fabricCanvas.on("selection:created", () => {
    textEditArmedObjId = "";
    focusedLockedObject = null;
    applySelectionStyles();
    updateStylePanelVisibility();
    updateSelectionToolbar();
    updateRotateButtonState();
    requestAnimationFrame(updateSelectionToolbar);
  });

  fabricCanvas.on("selection:updated", () => {
    textEditArmedObjId = "";
    focusedLockedObject = null;
    applySelectionStyles();
    updateStylePanelVisibility();
    updateSelectionToolbar();
    updateRotateButtonState();
    requestAnimationFrame(updateSelectionToolbar);
  });

  fabricCanvas.on("selection:cleared", () => {
    textEditArmedObjId = "";
    updateSelectionToolbar();
    updateRotateButtonState();
  });

  fabricCanvas.on("mouse:dblclick", (opt) => {
    if (!canEdit || currentTool !== "select") return;
    const target = opt && opt.target;
    if (isStickerObject(target)) enterStickerEditMode(target);
  });

  if (stickerEditOverlay) {
    stickerEditOverlay.addEventListener("blur", () => exitStickerEditMode(true));
    stickerEditOverlay.addEventListener("input", fitStickerOverlayFont);
    stickerEditOverlay.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        exitStickerEditMode(false);
      }
    });
  }

  document.querySelectorAll(".tool-btn[data-tool]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const t = btn.dataset.tool;

      if (t === "pencil") {
        if (currentTool === "pencil" && pencilPanel.classList.contains("active")) {
          hidePanels();
          return;
        }
        setTool("pencil");
        hidePanels();
        pencilPanel.classList.add("active");
        placePanelNear(pencilPanel, pencilToolBtn);
        return;
      }

      if (t === "text") {
        if (currentTool === "text" && textPanel.classList.contains("active")) {
          hidePanels();
          return;
        }
        setTool("text");
        hidePanels();
        textPanel.classList.add("active");
        placePanelNear(textPanel, textToolBtn);
        return;
      }

      if (t === "shape") {
        if (currentTool === "shape" && currentShapeType !== "sticker" && shapePanel && shapePanel.classList.contains("active")) {
          hidePanels();
          return;
        }
        // The "Фигуры" button always means an actual shape, never the
        // sticker tool - if sticker was the last thing picked, fall back to
        // a real shape so this button doesn't light up the sticker button.
        if (currentShapeType === "sticker") setShapeType("arrow");
        setTool("shape");
        hidePanels();
        if (shapePanel) {
          shapePanel.classList.add("active");
          placePanelNear(shapePanel, shapeToolBtn || btn);
        }
        return;
      }

      hidePanels();
      setTool(t);
    });
  });

  document.querySelectorAll(".shape-btn[data-shape]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setShapeType(btn.dataset.shape || "rect");
      setTool("shape");
    });
  });

  imageToolBtn.addEventListener("click", () => {
    if (!canEdit) return;
    hidePanels();
    hiddenImageInput.click();
  });

  if (stickerToolBtn) {
    stickerToolBtn.addEventListener("click", () => {
      if (!canEdit) return;
      hidePanels();
      setShapeType("sticker");
      setTool("shape");
    });
  }

  if (selColorBtn) {
    selColorBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleSelPopover(selColorPopover, selColorBtn);
    });
  }
  if (selFillBtn) {
    selFillBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleSelPopover(selFillPopover, selFillBtn);
    });
  }
  if (selStrokeBtn) {
    selStrokeBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleSelPopover(selStrokePopover, selStrokeBtn);
    });
  }
  if (selCornerBtn) {
    selCornerBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleSelPopover(selCornerPopover, selCornerBtn);
    });
  }
  if (selLockBtn) selLockBtn.addEventListener("click", toggleSelectionLock);
  if (selDeleteBtn) {
    selDeleteBtn.addEventListener("click", () => {
      if (!canEdit) return;
      deleteActiveObjects();
    });
  }

  hiddenImageInput.addEventListener("change", () => {
    const file = hiddenImageInput.files && hiddenImageInput.files[0];
    if (!file) {
      hiddenImageInput.value = "";
      return;
    }
    if (!isImageFile(file)) {
      showBoardError("Неподдерживаемый формат файла");
      hiddenImageInput.value = "";
      return;
    }
    const center = worldCenterOfViewport();
    addImageFileAtWorldPoint(file, center.x, center.y).catch((err) => {
      showBoardError(err?.message || "Не удалось загрузить изображение");
    });
    hiddenImageInput.value = "";
  });

  function updateSvByClient(clientX, clientY) {
    if (!customColorSv) return;
    const rect = customColorSv.getBoundingClientRect();
    const x = Math.max(0, Math.min(rect.width, clientX - rect.left));
    const y = Math.max(0, Math.min(rect.height, clientY - rect.top));
    customColorState.s = rect.width > 0 ? x / rect.width : 0;
    customColorState.v = rect.height > 0 ? 1 - y / rect.height : 0;
    syncCustomInputsFromState();
    applyCustomColorPick(colorFromCustomStateHex());
  }

  function updateHueByClient(clientX) {
    if (!customColorHue) return;
    const rect = customColorHue.getBoundingClientRect();
    const x = Math.max(0, Math.min(rect.width, clientX - rect.left));
    const nextHue = rect.width > 0 ? (x / rect.width) * 360 : 0;
    customColorState.h = Math.max(0, Math.min(359.999, nextHue));
    syncCustomInputsFromState();
    applyCustomColorPick(colorFromCustomStateHex());
  }

  if (customColorSv) {
    customColorSv.addEventListener("pointerdown", (e) => {
      customColorDragMode = "sv";
      updateSvByClient(e.clientX, e.clientY);
      customColorSv.setPointerCapture?.(e.pointerId);
    });
  }
  if (customColorHue) {
    customColorHue.addEventListener("pointerdown", (e) => {
      customColorDragMode = "hue";
      updateHueByClient(e.clientX);
      customColorHue.setPointerCapture?.(e.pointerId);
    });
  }
  window.addEventListener("pointermove", (e) => {
    if (customColorDragMode === "sv") updateSvByClient(e.clientX, e.clientY);
    if (customColorDragMode === "hue") updateHueByClient(e.clientX);
  });
  window.addEventListener("pointerup", () => {
    customColorDragMode = "";
  });

  window.addEventListener("dragover", (e) => {
    if (hasDraggedFiles(e.dataTransfer)) e.preventDefault();
  });
  window.addEventListener("drop", (e) => {
    if (hasDraggedFiles(e.dataTransfer)) e.preventDefault();
  });

  boardWrap.addEventListener("dragover", (e) => {
    if (!hasDraggedFiles(e.dataTransfer)) return;
    e.preventDefault();
    if (e.dataTransfer && canEdit) e.dataTransfer.dropEffect = "copy";
  });
  boardWrap.addEventListener("drop", (e) => {
    e.preventDefault();
    e.stopPropagation();
    const files = [...(e.dataTransfer?.files || [])];
    if (!files.length) return;
    if (!canEdit) return;
    const file = files.find((f) => isImageFile(f));
    if (!file) {
      showBoardError("Неподдерживаемый формат файла");
      return;
    }
    const p = activeDropWorldPoint(e.clientX, e.clientY);
    addImageFileAtWorldPoint(file, p.x, p.y).catch((err) => {
      showBoardError(err?.message || "Не удалось загрузить изображение");
    });
  });

  if (strokeWidthEl) {
    strokeWidthEl.addEventListener("input", () => {
      updateBrush();
      applyStyleToSelection();
    });
  }

  if (textSizeEl) {
    textSizeEl.addEventListener("input", () => {
      const active = fabricCanvas.getActiveObject();
      if (active && active.type === "i-text") {
        active.set({ fontSize: Number(textSizeEl.value) });
        fabricCanvas.requestRenderAll();
        enqueueUpdateOp(active);
      }
    });
  }

  if (cornerRadiusEl) {
    cornerRadiusEl.addEventListener("input", () => {
      applyCornerRadiusToSelection(cornerRadiusEl.value);
      updateUndoRedoDockVisibility();
    });
  }

  if (selectionStrokeWidthEl) {
    selectionStrokeWidthEl.addEventListener("input", () => {
      applySelectionStrokeWidth(selectionStrokeWidthEl.value);
    });
  }


  undoBtn.addEventListener("click", () => socket.emit("undo"));
  redoBtn.addEventListener("click", () => socket.emit("redo"));
  // Click still nudges by 15° (see rotateDragState.moved below), but
  // press-and-hold now lets you drag to rotate freely, tracking the pointer
  // angle around the object's center - Shift snaps to 15° increments while
  // dragging, same as the old click-only behavior's step size.
  let rotateDragState = null;

  function startRotateDrag(e) {
    const active = fabricCanvas.getActiveObject();
    if (!active || !canEdit) return;
    active.setCoords();
    const center = active.getCenterPoint();
    const centerScreen = screenFromWorld(center.x, center.y);
    const canvasRect = canvasEl.getBoundingClientRect();
    const centerX = canvasRect.left + centerScreen.x;
    const centerY = canvasRect.top + centerScreen.y;
    rotateDragState = {
      target: active,
      centerX,
      centerY,
      startPointerAngle: Math.atan2(e.clientY - centerY, e.clientX - centerX) * (180 / Math.PI),
      startObjectAngle: active.angle || 0,
      startClientX: e.clientX,
      startClientY: e.clientY,
      moved: false,
    };
    if (typeof selRotateBtn.setPointerCapture === "function") {
      try { selRotateBtn.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
    }
  }

  function onRotateDragMove(e) {
    if (!rotateDragState) return;
    const dx = e.clientX - rotateDragState.startClientX;
    const dy = e.clientY - rotateDragState.startClientY;
    if (!rotateDragState.moved && Math.hypot(dx, dy) < 4) return;
    rotateDragState.moved = true;
    const currentPointerAngle = Math.atan2(
      e.clientY - rotateDragState.centerY,
      e.clientX - rotateDragState.centerX,
    ) * (180 / Math.PI);
    let nextAngle = rotateDragState.startObjectAngle + (currentPointerAngle - rotateDragState.startPointerAngle);
    if (e.shiftKey) nextAngle = Math.round(nextAngle / 15) * 15;
    rotateDragState.target.rotate(nextAngle);
    rotateDragState.target.setCoords();
    fabricCanvas.requestRenderAll();
  }

  function endRotateDrag() {
    if (!rotateDragState) return;
    const { target, moved } = rotateDragState;
    if (!moved) {
      target.rotate((target.angle || 0) + 15);
      target.setCoords();
      fabricCanvas.requestRenderAll();
    }
    rotateDragState = null;
    enqueueSelectionUpdates();
    updateSelectionToolbar();
  }

  if (selRotateBtn) {
    selRotateBtn.addEventListener("pointerdown", (e) => {
      e.preventDefault();
      e.stopPropagation();
      startRotateDrag(e);
    });
    window.addEventListener("pointermove", onRotateDragMove);
    window.addEventListener("pointerup", endRotateDrag);
  }
  clearBtn.addEventListener("click", async () => {
    const ok = await showConfirmModal({
      title: "Очистить доску?",
      body: "Все объекты на доске будут удалены без возможности восстановления.",
      confirmLabel: "Очистить",
    });
    if (ok) socket.emit("clear");
  });
  bgBtn.addEventListener("click", toggleBackground);

  if (miroImportBtn) {
    miroImportBtn.addEventListener("click", () => {
      if (!canEdit) return;
      openMiroImportModal();
    });
  }
  if (miroModalCancel) miroModalCancel.addEventListener("click", closeModal);
  if (miroModalOk) {
    miroModalOk.addEventListener("click", async () => {
      const miroBoardId = extractMiroBoardId(miroBoardUrlInput.value);
      const miroToken = (miroTokenInput.value || "").trim();
      if (!miroBoardId) {
        setMiroModalStatus("Укажите ссылку или ID доски Miro", "error");
        return;
      }
      if (!miroToken) {
        setMiroModalStatus("Укажите personal access token", "error");
        return;
      }

      if (miroRememberToken && miroRememberToken.checked) {
        localStorage.setItem(MIRO_TOKEN_STORAGE_KEY, miroToken);
      } else {
        localStorage.removeItem(MIRO_TOKEN_STORAGE_KEY);
      }

      miroModalOk.disabled = true;
      miroModalCancel.disabled = true;
      if (miroModalOkLabel) miroModalOkLabel.textContent = "Импортируем…";
      setMiroModalStatus("Импортируем доску из Miro…", "info");
      try {
        const headers = { "Content-Type": "application/json" };
        if (pageToken) headers["Authorization"] = `Bearer ${pageToken}`;
        const res = await fetch(`/api/board/${encodeURIComponent(boardId)}/import/miro`, {
          method: "POST",
          headers,
          body: JSON.stringify({ miro_board_id: miroBoardId, miro_token: miroToken }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          setMiroModalStatus(data.detail || "Не удалось импортировать доску из Miro", "error");
          return;
        }
        setMiroModalStatus(`Импортировано объектов: ${data.imported}, пропущено: ${data.skipped}`, "success");
        showBoardNotice(`Импортировано объектов: ${data.imported}, пропущено: ${data.skipped}`, "success");
        setTimeout(closeModal, 1200);
      } catch (err) {
        setMiroModalStatus(err?.message || "Не удалось импортировать доску из Miro", "error");
      } finally {
        miroModalOk.disabled = false;
        miroModalCancel.disabled = false;
        if (miroModalOkLabel) miroModalOkLabel.textContent = "Импортировать";
      }
    });
  }
  zoomOutBtn.addEventListener("click", () => zoomByFactor(0.9, fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2));
  zoomInBtn.addEventListener("click", () => zoomByFactor(1.1, fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2));
  zoomCenterBtn.addEventListener("click", resetView);

  if (miniCollapseBtn && miniWrap) {
    // Small screens only (see CSS) - reclaim canvas space by hiding the
    // minimap preview while keeping the zoom controls one tap away.
    miniCollapseBtn.addEventListener("click", () => {
      miniWrap.classList.toggle("is-collapsed");
    });
  }

  miniMapEl.addEventListener("pointerdown", (e) => {
    e.preventDefault();
    fabricCanvas.discardActiveObject();
    fabricCanvas.requestRenderAll();
    updateStylePanelVisibility();
    miniDragging = true;
    miniMapEl.style.cursor = "grabbing";
    miniMapEl.setPointerCapture(e.pointerId);
    handleMiniPointer(e.clientX, e.clientY);
  });

  miniMapEl.addEventListener("pointermove", (e) => {
    if (!miniDragging) return;
    handleMiniPointer(e.clientX, e.clientY);
  });

  miniMapEl.addEventListener("pointerup", (e) => {
    miniDragging = false;
    miniMapEl.style.cursor = "grab";
    try {
      miniMapEl.releasePointerCapture(e.pointerId);
    } catch (_) {
      // ignore
    }
  });

  window.addEventListener("keydown", (e) => {
    const targetTag = (e.target && e.target.tagName || "").toLowerCase();
    const isInput = targetTag === "input" || targetTag === "textarea";

    if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === "z") {
      e.preventDefault();
      socket.emit("undo");
      return;
    }

    if ((e.ctrlKey || e.metaKey) && ((e.shiftKey && e.key.toLowerCase() === "z") || e.key.toLowerCase() === "y")) {
      e.preventDefault();
      socket.emit("redo");
      return;
    }

    if (!isInput && (e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === "c") {
      if (currentTool === "select" && !isEditingTextNow()) {
        if (copyActiveSelection()) e.preventDefault();
      }
      return;
    }

    if (!isInput && (e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === "v") {
      if (currentTool === "select" && !isEditingTextNow()) {
        e.preventDefault();
        pasteCopiedSelection();
      }
      return;
    }

    if (!isInput && (e.key === "Delete" || e.key === "Backspace")) {
      const active = fabricCanvas.getActiveObject();
      if (active) {
        e.preventDefault();
        deleteActiveObjects();
      }
    }
  });

  window.addEventListener("resize", () => {
    resizeCanvas(false);
    syncAllITextLayouts();
    hidePanels();
    updateSelectionToolbar();
  });

  if (document.fonts && document.fonts.ready && typeof document.fonts.ready.then === "function") {
    document.fonts.ready.then(() => {
      syncAllITextLayouts();
    }).catch(() => {});
  }

  document.addEventListener("click", (e) => {
    const customOpen = !!(customColorPanel && customColorPanel.classList.contains("active"));
    const insideCustomPanel = !!(customColorPanel && customColorPanel.contains(e.target));
    const onCustomTrigger = !!e.target.closest(".swatch-custom-trigger");
    if (customOpen && !insideCustomPanel && !onCustomTrigger) {
      customColorPanel.classList.remove("active");
      return;
    }
    const inside = e.target.closest("#toolbar") || e.target.closest(".floating-panel");
    if (!inside) hidePanels();

    const insideSelToolbar = e.target.closest(".sel-toolbar") || e.target.closest(".sel-popover");
    if (!insideSelToolbar) closeSelPopovers();
  });

  boardWrap.addEventListener("mousedown", (e) => {
    if (e.button === 1) {
      e.preventDefault();
    }
  });

  boardWrap.addEventListener("auxclick", (e) => {
    if (e.button === 1) {
      e.preventDefault();
    }
  });

  boardWrap.addEventListener("touchstart", (e) => {
    if (supportsPointerEvents) return;
    if (e.touches.length >= 2) {
      e.preventDefault();
      if (typeof e.stopPropagation === "function") e.stopPropagation();
      startPinch(e.touches);
    }
  }, { passive: false, capture: true });

  boardWrap.addEventListener("touchmove", (e) => {
    if (supportsPointerEvents) return;
    if (!pinchMode || e.touches.length < 2) return;
    e.preventDefault();
    if (typeof e.stopPropagation === "function") e.stopPropagation();
    const [t1, t2] = e.touches;
    updatePinchWithPoints(
      { x: t1.clientX, y: t1.clientY },
      { x: t2.clientX, y: t2.clientY },
    );
  }, { passive: false, capture: true });

  boardWrap.addEventListener("touchend", (e) => {
    if (supportsPointerEvents) return;
    if (pinchMode && e.touches.length < 2) {
      e.preventDefault();
      if (typeof e.stopPropagation === "function") e.stopPropagation();
      stopPinch();
    }
  }, { passive: false, capture: true });

  boardWrap.addEventListener("touchcancel", (e) => {
    if (supportsPointerEvents) return;
    if (typeof e.preventDefault === "function") e.preventDefault();
    if (typeof e.stopPropagation === "function") e.stopPropagation();
    stopPinch();
  }, { passive: false, capture: true });

  boardWrap.addEventListener("pointerdown", (e) => {
    if (!supportsPointerEvents || e.pointerType !== "touch") return;
    activeTouchPointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (activeTouchPointers.size < 2) return;
    e.preventDefault();
    if (typeof e.stopPropagation === "function") e.stopPropagation();
    const points = [...activeTouchPointers.values()];
    if (!pinchMode) startPinchWithPoints(points[0], points[1]);
  }, { passive: false, capture: true });

  boardWrap.addEventListener("pointermove", (e) => {
    if (!supportsPointerEvents || e.pointerType !== "touch") return;
    if (!activeTouchPointers.has(e.pointerId)) return;
    activeTouchPointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (activeTouchPointers.size < 2) return;
    e.preventDefault();
    if (typeof e.stopPropagation === "function") e.stopPropagation();
    const points = [...activeTouchPointers.values()];
    if (!pinchMode) {
      startPinchWithPoints(points[0], points[1]);
      return;
    }
    updatePinchWithPoints(points[0], points[1]);
  }, { passive: false, capture: true });

  boardWrap.addEventListener("pointerup", (e) => {
    if (!supportsPointerEvents || e.pointerType !== "touch") return;
    activeTouchPointers.delete(e.pointerId);
    if (pinchMode && activeTouchPointers.size < 2) {
      e.preventDefault();
      if (typeof e.stopPropagation === "function") e.stopPropagation();
      stopPinch();
    }
  }, { passive: false, capture: true });

  boardWrap.addEventListener("pointercancel", (e) => {
    if (!supportsPointerEvents || e.pointerType !== "touch") return;
    activeTouchPointers.delete(e.pointerId);
    if (typeof e.preventDefault === "function") e.preventDefault();
    if (typeof e.stopPropagation === "function") e.stopPropagation();
    if (pinchMode && activeTouchPointers.size < 2) stopPinch();
  }, { passive: false, capture: true });

  // Belt-and-braces on top of Fabric's own mouse:wheel preventDefault (see
  // above): Ctrl/Cmd+scroll and trackpad pinch both fire as a native `wheel`
  // event with ctrlKey set, and if that ever reaches the browser undefended
  // (e.g. a listener elsewhere on the page registered passively) it zooms
  // the whole page instead of just the canvas - locking the toolbar off
  // screen with no way back except resetting browser zoom manually. Bound to
  // document (not just boardWrap) since the toolbar/panels are separate
  // fixed-position siblings, not children of boardWrap - Ctrl+scroll over
  // them needs blocking too.
  document.addEventListener("wheel", (e) => {
    if (e.ctrlKey || e.metaKey) e.preventDefault();
  }, { passive: false, capture: true });

  // Safari doesn't fire wheel+ctrlKey for trackpad pinch - it uses these
  // non-standard gesture events instead. Just preventDefault-ing them (as a
  // prior version of this code did) blocked Safari's native pinch-to-zoom-
  // the-page fallback without ever wiring the gesture to actually zoom the
  // canvas, which silently took away pinch-zoom for Safari trackpad users
  // entirely - drive zoomByFactor from the gesture's own scale instead so
  // the pinch still does something.
  let gestureStartZoom = 1;
  document.addEventListener("gesturestart", (e) => {
    e.preventDefault();
    gestureStartZoom = fabricCanvas.getZoom();
  }, { passive: false });
  document.addEventListener("gesturechange", (e) => {
    e.preventDefault();
    if (!boardWrap.contains(e.target)) return;
    const rect = canvasEl.getBoundingClientRect();
    const targetZoom = Math.max(0.08, Math.min(24, gestureStartZoom * e.scale));
    const factor = targetZoom / fabricCanvas.getZoom();
    zoomByFactor(factor, e.clientX - rect.left, e.clientY - rect.top);
  }, { passive: false });
  document.addEventListener("gestureend", (e) => e.preventDefault(), { passive: false });

  fabricCanvas.on("after:render", () => {
    updateSelectionToolbar();
  });

  function updateLastSeenSeq(ops) {
    if (!Array.isArray(ops)) return;
    let maxSeq = lastSeenSeqId;
    for (const op of ops) {
      if (op && typeof op.seq === "number" && op.seq > maxSeq) {
        maxSeq = op.seq;
      }
    }
    if (maxSeq > lastSeenSeqId) {
      lastSeenSeqId = maxSeq;
      socket.auth.last_seen_seq = lastSeenSeqId;
    }
  }

  socket.on("connect", () => {
    isSocketConnected = true;
  });

  socket.on("disconnect", (reason) => {
    isSocketConnected = false;
    isBoardInitialized = false;
    hadConnectionDrop = true;
    canEdit = false;
    canClear = false;
    // Keep any not-yet-sent ops (queued in the 35ms batch window) instead of
    // discarding them - they never reached the server, so without this the
    // user's last edit(s) right before a drop would simply vanish. They are
    // flushed once the socket reconnects and resyncs, see socket.on("init").
    if (pendingOpsTimer) {
      clearTimeout(pendingOpsTimer);
      pendingOpsTimer = null;
    }
    applyEditPermissions();
    showConnectionNotice("Связь потеряна. Пытаемся переподключиться…", 0);
    for (const clientId of remoteCursors.keys()) removeRemoteCursor(clientId);
    if (reason === "io server disconnect") socket.connect();
  });

  socket.on("connect_error", async (err) => {
    isSocketConnected = false;
    isBoardInitialized = false;
    hadConnectionDrop = true;
    canEdit = false;
    canClear = false;
    applyEditPermissions();
    if (isAuthConnectError(err)) {
      const refreshed = await refreshWsToken(true);
      if (refreshed && !socket.connected) {
        socket.connect();
        return;
      }
      showConnectionNotice("Сессия подключения истекла. Обновите страницу.", 0, "error");
      return;
    }
    showConnectionNotice("Не удается подключиться к доске. Проверяем связь…", 3000);
  });

  socket.on("init", (msg) => {
    isSocketConnected = true;
    isBoardInitialized = true;
    myClientId = msg.client_id || "";
    myUserId = msg.user_id || myUserId;
    myJwtRole = msg.jwt_role || myJwtRole;
    debugForceEdit = !!msg.debug_force_edit;
    boardRoleCanEdit = msg.role === "owner" || msg.role === "editor";
    allowStudentsDraw = !!msg.allow_students_draw;
    canEdit = debugForceEdit
      ? true
      : (typeof msg.can_edit === "boolean" ? msg.can_edit : boardRoleCanEdit);
    canClear = !!msg.can_clear;

    clearBtn.style.display = canClear ? "inline-flex" : "none";

    applyEditPermissions();
    if (hadConnectionDrop) {
      showConnectionNotice("Соединение восстановлено", 0, "success");
      hadConnectionDrop = false;
    }

    if (msg.last_seq_id) {
      lastSeenSeqId = msg.last_seq_id;
      socket.auth.last_seen_seq = lastSeenSeqId;
    }

    if (msg.status === "sync") {
      // Keep existing canvas state
    } else {
      applyCanvasState(msg.canvas_json || { version: "6.0.0", objects: [] });
    }
    socket.emit("history_state_request");

    // Resend any ops that were queued but never made it out before a
    // disconnect. add/update/remove ops are idempotent upserts keyed by
    // obj_id, so replaying them here (even after a full canvas reload) is
    // safe and restores edits that would otherwise have been lost.
    if (pendingOps.length && canEdit) {
      const batch = pendingOps;
      pendingOps = [];
      socket.emit("batch_update", { ops: batch });
    } else {
      pendingOps = [];
    }
  });

  socket.on("board_policy", (msg) => {
    if (!isSocketConnected || !isBoardInitialized) return;
    if (debugForceEdit) {
      canEdit = true;
      applyEditPermissions();
      return;
    }
    allowStudentsDraw = !!(msg && msg.allow_students_draw);
    canEdit = (myJwtRole === "moderator") || (allowStudentsDraw && boardRoleCanEdit);
    applyEditPermissions();
  });

  // Fires when an owner grants/revokes this user's board role while they're
  // already connected, so access changes apply immediately instead of only
  // after a reconnect/refresh.
  socket.on("role_update", (msg) => {
    if (!isSocketConnected || !isBoardInitialized || debugForceEdit) return;
    boardRoleCanEdit = msg && (msg.role === "owner" || msg.role === "editor");
    canEdit = (myJwtRole === "moderator") || (allowStudentsDraw && boardRoleCanEdit);
    applyEditPermissions();
  });

  socket.on("batch_update", (msg) => {
    if (msg && Array.isArray(msg.ops)) {
      applyRemoteOps(msg.ops);
      updateLastSeenSeq(msg.ops);
    }
  });

  socket.on("update", (msg) => {
    if (msg.canvas_json) applyCanvasState(msg.canvas_json);
    if (msg.last_seq_id) {
      lastSeenSeqId = msg.last_seq_id;
      socket.auth.last_seen_seq = lastSeenSeqId;
    }
  });

  socket.on("clear", (msg) => {
    applyCanvasState(msg.canvas_json || { version: "6.0.0", objects: [] });
    if (msg.last_seq_id) {
      lastSeenSeqId = msg.last_seq_id;
      socket.auth.last_seen_seq = lastSeenSeqId;
    } else {
      lastSeenSeqId = 0;
      socket.auth.last_seen_seq = 0;
    }
  });

  socket.on("cursor", (msg) => {
    if (!msg.client_id || msg.client_id === myClientId) return;
    const c = ensureRemoteCursor(msg.client_id, msg.username);
    c.tx = Number(msg.x) || 0;
    c.ty = Number(msg.y) || 0;
    if (!Number.isFinite(c.x) || !Number.isFinite(c.y)) {
      c.x = c.tx;
      c.y = c.ty;
    }
    c.ts = Date.now();
  });

  socket.on("cursor_remove", (msg) => {
    if (!msg || !msg.client_id) return;
    removeRemoteCursor(msg.client_id);
  });

  socket.on("history_state", (msg) => {
    undoBtn.disabled = !msg.undo_available;
    redoBtn.disabled = !msg.redo_available;
  });

  socket.on("error_msg", (msg) => {
    showBoardError(msg?.message || "Socket error");
  });

  setInterval(() => {
    const cutoff = Date.now() - 12000;
    for (const [clientId, item] of remoteCursors.entries()) {
      if (item.ts < cutoff) removeRemoteCursor(clientId);
    }
  }, 3000);

  setInterval(() => {
    if (!socket.connected) return;
    if (tokenExpiresSoon(wsRefreshTokenExp, 25)) {
      refreshWsToken(true).catch(() => false);
      return;
    }
    refreshWsToken(false).catch(() => false);
  }, 30000);

  setupObjectStyles();
  syncCustomColorInputs(currentColor);
  buildPalette();
  setShapeType("arrow");
  setBackground("grid");
  resizeCanvas(true);
  ensureCanvasTransparentBackground();
  updateUndoRedoDockVisibility();
  applyEditPermissions();
  setTool("select");
  ensureCursorAnimation();
})();
