(() => {
  const body = document.body;
  const boardId = body.dataset.boardId;
  const token = body.dataset.token || new URLSearchParams(location.search).get("token") || "";
  const localUsername = body.dataset.username || "user";

  const boardWrap = document.getElementById("boardWrap");
  const canvasEl = document.getElementById("canvas");
  const cursorLayer = document.getElementById("cursorLayer");

  const pencilToolBtn = document.getElementById("pencilTool");
  const shapeToolBtn = document.getElementById("shapeTool");
  const textToolBtn = document.getElementById("textTool");
  const imageToolBtn = document.getElementById("imageTool");
  const mobileImageBtn = document.getElementById("mobileImageBtn");
  const undoBtn = document.getElementById("undoBtn");
  const redoBtn = document.getElementById("redoBtn");
  const undoRedoDock = document.getElementById("undoRedoDock");
  const rotateBtn = document.getElementById("rotateBtn");
  const mobileRotateBtn = document.getElementById("mobileRotateBtn");
  const clearBtn = document.getElementById("clearBtn");
  const mobileClearBtn = document.getElementById("mobileClearBtn");
  const bgBtn = document.getElementById("bgBtn");
  const mobileBgBtn = document.getElementById("mobileBgBtn");
  const mobileMoreBtn = document.getElementById("mobileMoreBtn");
  const hiddenImageInput = document.getElementById("hiddenImageInput");

  const pencilPanel = document.getElementById("pencilPanel");
  const textPanel = document.getElementById("textPanel");
  const shapePanel = document.getElementById("shapePanel");
  const stylePanel = document.getElementById("stylePanel");
  const mobileMorePanel = document.getElementById("mobileMorePanel");
  const palette = document.getElementById("palette");

  const strokeWidthEl = document.getElementById("strokeWidth");
  const textSizeEl = document.getElementById("textSize");
  const cornerRadiusEl = document.getElementById("cornerRadius");

  const zoomOutBtn = document.getElementById("zoomOutBtn");
  const zoomInBtn = document.getElementById("zoomInBtn");
  const zoomCenterBtn = document.getElementById("zoomCenterBtn");
  const miniMapEl = document.getElementById("miniMap");
  const miniCtx = miniMapEl.getContext("2d");

  if (!boardWrap || !canvasEl || !miniMapEl || !miniCtx) {
    console.error("Whiteboard init failed: required DOM nodes are missing.");
    return;
  }

  const paletteColors = ["#1f2937", "#2563eb", "#0f766e", "#7c3aed", "#be123c", "#ea580c", "#16a34a", "#525252"];
  const cursorColors = ["#0d6efd", "#7c3aed", "#dc2626", "#0f766e", "#ea580c", "#9333ea", "#1d4ed8", "#0891b2"];
  const GRID_WORLD_SIZE = 24;
  const GRID_BG_IMAGE =
    "linear-gradient(90deg, rgba(17,24,39,0.045) 1px, transparent 1px), linear-gradient(rgba(17,24,39,0.045) 1px, transparent 1px)";

  let currentTool = "select";
  let currentShapeType = "rect";
  let currentColor = paletteColors[0];
  let currentBackground = "grid";
  let canEdit = true;
  let boardRoleCanEdit = true;
  let canClear = false;
  let myJwtRole = "";
  let myClientId = "";
  let myUserId = "";

  let isRemoteApplying = false;
  let suppressBroadcast = false;
  let pendingOpsTimer = null;
  let pendingOps = [];
  let remoteOpsChain = Promise.resolve();
  let cursorAnimFrame = 0;
  let pendingTextSyncTimer = null;

  let panMode = false;
  let panLast = { x: 0, y: 0 };
  let drawingShape = null;
  let drawingStart = null;
  let erasing = false;

  let pinchMode = false;
  let pinchDist = 0;
  let pinchCenter = null;

  let skipNextTextCreate = false;

  let lastCursorSentAt = 0;
  const remoteCursors = new Map();

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

  const socket = io({
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 500,
    reconnectionDelayMax: 4000,
    timeout: 20000,
    auth: { token, board_id: boardId, history_id: historyClientId },
  });

  const fabricCanvas = new fabric.Canvas(canvasEl, {
    selection: true,
    preserveObjectStacking: true,
  });
  fabricCanvas.uniformScaling = false;

  function setupObjectStyles() {
    fabricCanvas.selectionColor = "rgba(13,110,253,0.04)";
    fabricCanvas.selectionBorderColor = "#0d6efd";
    fabricCanvas.selectionLineWidth = 1.5;

    fabric.Object.prototype.set({
      borderColor: "#0d6efd",
      borderScaleFactor: 1.5,
      borderDashArray: null,
      cornerColor: "#ffffff",
      cornerStrokeColor: "#0d6efd",
      transparentCorners: false,
      cornerStyle: "rect",
      cornerSize: 10,
      touchCornerSize: 28,
      borderOpacityWhenMoving: 0.95,
      lockRotation: true,
      lockUniScaling: false,
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

  function updateRotateButtonState() {
    const disabled = !canEdit || !fabricCanvas.getActiveObject();
    if (rotateBtn) rotateBtn.disabled = disabled;
    if (mobileRotateBtn) mobileRotateBtn.disabled = disabled;
  }

  function applyEditPermissions() {
    document.querySelectorAll(".tool-btn[data-tool]").forEach((btn) => {
      const t = btn.dataset.tool;
      btn.disabled = !canEdit && t !== "select" && t !== "hand";
    });
    imageToolBtn.disabled = !canEdit;
    if (mobileImageBtn) mobileImageBtn.disabled = !canEdit;
    if (mobileBgBtn) mobileBgBtn.disabled = false;
    syncObjectInteractivity();
    updateRotateButtonState();
  }

  function setBackground(mode) {
    currentBackground = mode;
    if (mode === "grid") {
      boardWrap.style.backgroundImage = GRID_BG_IMAGE;
      boardWrap.style.backgroundColor = "#ffffff";
      bgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/grid-3x3-gap-fill.svg\')"></span>';
      if (mobileBgBtn) mobileBgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/grid-3x3-gap-fill.svg\')"></span>';
    } else {
      boardWrap.style.backgroundImage = "none";
      boardWrap.style.backgroundColor = "#ffffff";
      bgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/border-all.svg\')"></span>';
      if (mobileBgBtn) mobileBgBtn.innerHTML = '<span class="bi-local" style="--icon:url(\'/static/icons/border-all.svg\')"></span>';
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

  function buildPalette() {
    palette.innerHTML = "";
    for (const color of paletteColors) {
      const btn = document.createElement("button");
      btn.className = `swatch${color === currentColor ? " active" : ""}`;
      btn.style.background = color;
      btn.type = "button";
      btn.title = color;
      btn.addEventListener("click", () => {
        currentColor = color;
        buildPalette();
        updateBrush();
        applyStyleToSelection();
      });
      palette.appendChild(btn);
    }
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
    textPanel.classList.remove("active");
    if (shapePanel) shapePanel.classList.remove("active");
    if (mobileMorePanel) mobileMorePanel.classList.remove("active");
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
      btn.classList.toggle("active", btn.dataset.tool === currentTool);
    });

    fabricCanvas.isDrawingMode = canEdit && currentTool === "pencil";
    fabricCanvas.selection = canEdit && currentTool === "select";
    fabricCanvas.skipTargetFind = !(canEdit && (currentTool === "select" || currentTool === "eraser"));

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

  function setShapeType(shape) {
    currentShapeType = shape;
    document.querySelectorAll(".shape-btn[data-shape]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.shape === currentShapeType);
    });
  }

  function updateUndoRedoDockVisibility() {
    if (!undoRedoDock) return;
    const hide = !!(stylePanel && stylePanel.classList.contains("active"));
    undoRedoDock.classList.toggle("is-hidden", hide);
  }

  function syncObjectInteractivity() {
    const selectMode = canEdit && currentTool === "select";
    const eraseMode = canEdit && currentTool === "eraser";

    fabricCanvas.forEachObject((obj) => {
      obj.selectable = selectMode;
      obj.evented = selectMode || eraseMode;
    });

    if (!selectMode) {
      fabricCanvas.discardActiveObject();
    }

    updateRotateButtonState();
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
        borderColor: "#0d6efd",
        borderDashArray: [6, 3],
        lockRotation: true,
        cornerStyle: "rect",
        transparentCorners: false,
        cornerColor: "#ffffff",
        cornerStrokeColor: "#0d6efd",
        cornerSize: 10,
        touchCornerSize: 28,
      });
      setCornerOnlyControls(active);
    } else {
      active.set({
        hasControls: true,
        hasBorders: true,
        borderColor: "#0d6efd",
        borderDashArray: [6, 3],
        cornerColor: "#ffffff",
        cornerStrokeColor: "#0d6efd",
        transparentCorners: false,
        cornerStyle: "rect",
        cornerSize: 10,
        touchCornerSize: 28,
        lockRotation: true,
      });
      setCornerOnlyControls(active);
    }

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
    fabricCanvas.requestRenderAll();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
  }

  function resetView() {
    const w = fabricCanvas.getWidth();
    const h = fabricCanvas.getHeight();
    fabricCanvas.setViewportTransform([1, 0, 0, 1, w / 2, h / 2]);
    fabricCanvas.requestRenderAll();
    setZoomLabel();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
  }

  function zoomByFactor(factor, centerX, centerY) {
    let zoom = fabricCanvas.getZoom() * factor;
    zoom = Math.max(0.08, Math.min(24, zoom));
    const p = new fabric.Point(centerX, centerY);
    fabricCanvas.zoomToPoint(p, zoom);
    setZoomLabel();
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
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
    if (obj.type === "path" && (obj.shapeKind === "triangle" || obj.shapeKind === "diamond")) {
      return Number(obj.cornerRadius || 0);
    }
    return Number(obj.cornerRadius || obj.rx || 0);
  }

  function isRoundedPolygonShape(obj) {
    return !!(obj && obj.type === "path" && (obj.shapeKind === "triangle" || obj.shapeKind === "diamond"));
  }

  function isCornerRadiusSupportedObject(obj) {
    return !!(obj && (obj.type === "rect" || obj.type === "image" || isRoundedPolygonShape(obj)));
  }

  function roundedPolygonPathData(points, radius) {
    if (!Array.isArray(points) || points.length < 3) return "";
    const safeRadius = Math.max(0, Number(radius) || 0);
    if (safeRadius <= 0) {
      return `M ${points.map((p) => `${p.x} ${p.y}`).join(" L ")} Z`;
    }

    const corners = points.map((curr, i) => {
      const prev = points[(i - 1 + points.length) % points.length];
      const next = points[(i + 1) % points.length];
      const vPrev = { x: prev.x - curr.x, y: prev.y - curr.y };
      const vNext = { x: next.x - curr.x, y: next.y - curr.y };
      const lenPrev = Math.hypot(vPrev.x, vPrev.y) || 1;
      const lenNext = Math.hypot(vNext.x, vNext.y) || 1;
      const cut = Math.min(safeRadius, lenPrev / 2, lenNext / 2);
      const p1 = { x: curr.x + (vPrev.x / lenPrev) * cut, y: curr.y + (vPrev.y / lenPrev) * cut };
      const p2 = { x: curr.x + (vNext.x / lenNext) * cut, y: curr.y + (vNext.y / lenNext) * cut };
      return { curr, p1, p2 };
    });

    let d = `M ${corners[0].p1.x} ${corners[0].p1.y}`;
    for (let i = 0; i < corners.length; i += 1) {
      const c = corners[i];
      const next = corners[(i + 1) % corners.length];
      d += ` Q ${c.curr.x} ${c.curr.y} ${c.p2.x} ${c.p2.y}`;
      d += ` L ${next.p1.x} ${next.p1.y}`;
    }
    d += " Z";
    return d;
  }

  function createRoundedPolygonShape(type, x, y, width, height, strokeW, radius = 0) {
    const points = type === "triangle"
      ? [
          { x: width / 2, y: 0 },
          { x: width, y: height },
          { x: 0, y: height },
        ]
      : [
          { x: width / 2, y: 0 },
          { x: width, y: height / 2 },
          { x: width / 2, y: height },
          { x: 0, y: height / 2 },
        ];

    const maxR = Math.min(width, height) / 3;
    const safeRadius = Math.max(0, Math.min(maxR, Number(radius) || 0));
    const path = new fabric.Path(roundedPolygonPathData(points, safeRadius), {
      left: x,
      top: y,
      originX: "left",
      originY: "top",
      fill: "transparent",
      stroke: currentColor,
      strokeWidth: strokeW,
      strokeLineCap: "round",
      strokeLineJoin: "round",
      objectCaching: false,
    });
    path.shapeKind = type;
    path.cornerRadius = safeRadius;
    return path;
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
    } else if (isRoundedPolygonShape(obj)) {
      const width = Math.max(1, Number(obj.width || obj.getScaledWidth() || 1));
      const height = Math.max(1, Number(obj.height || obj.getScaledHeight() || 1));
      const replacement = createRoundedPolygonShape(
        obj.shapeKind,
        obj.left || 0,
        obj.top || 0,
        width,
        height,
        Number(obj.strokeWidth || 2),
        radius,
      );
      replacement.set({
        scaleX: obj.scaleX || 1,
        scaleY: obj.scaleY || 1,
        angle: obj.angle || 0,
        flipX: !!obj.flipX,
        flipY: !!obj.flipY,
        originX: obj.originX || "left",
        originY: obj.originY || "top",
        obj_id: obj.obj_id,
        author_id: obj.author_id,
        author_name: obj.author_name,
      });
      const canvas = obj.canvas;
      const active = fabricCanvas.getActiveObject();
      const wasSelected = active && captureActiveSelectionIds().includes(obj.obj_id);
      if (canvas) {
        canvas.remove(obj);
        canvas.add(replacement);
        if (wasSelected) {
          fabricCanvas.setActiveObject(replacement);
          applySelectionStyles();
        }
      }
    }
  }

  function syncRoundedImages() {
    fabricCanvas.forEachObject((obj) => {
      if (obj.type === "image") {
        applyImageCornerRadius(obj, getObjectCornerRadius(obj));
      }
    });
  }

  function getSelectionObjects() {
    const active = fabricCanvas.getActiveObject();
    if (!active) return [];
    const isMulti = isMultiSelectionObject(active);
    return isMulti ? active.getObjects() : [active];
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

  function updateStylePanelVisibility() {
    if (!stylePanel || !cornerRadiusEl) return;
    const objs = getSelectionObjects().filter((o) => isCornerRadiusSupportedObject(o));
    if (!objs.length) {
      stylePanel.classList.remove("active");
      updateUndoRedoDockVisibility();
      return;
    }
    const avg = objs.reduce((acc, o) => acc + getObjectCornerRadius(o), 0) / objs.length;
    cornerRadiusEl.value = String(Math.round(avg));
    stylePanel.classList.add("active");
    updateUndoRedoDockVisibility();
  }

  function applyCornerRadiusToSelection(value) {
    const radius = Number(value);
    if (!Number.isFinite(radius)) return;
    const objs = getSelectionObjects().filter((o) => isCornerRadiusSupportedObject(o));
    if (!objs.length) return;
    objs.forEach((obj) => {
      setObjectCornerRadius(obj, radius);
      obj.setCoords();
    });
    fabricCanvas.requestRenderAll();
    enqueueSelectionUpdates();
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

    if (isMultiSelectionObject(active)) {
      active.getObjects().forEach(applyOne);
    } else {
      applyOne(active);
    }

    fabricCanvas.requestRenderAll();
    enqueueSelectionUpdates();
  }

  function serializeObject(obj) {
    ensureObjMeta(obj);
    return obj.toObject(["obj_id", "author_id", "author_name", "cornerRadius", "shapeKind"]);
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
    if (isMultiSelectionObject(active) && typeof active.getObjects === "function") {
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
    if (!canEdit || isRemoteApplying || suppressBroadcast) return;
    const normalized = (Array.isArray(ops) ? ops : [ops]).filter((op) => op && typeof op === "object");
    if (!normalized.length) return;
    pendingOps.push(...normalized);
    if (pendingOpsTimer) clearTimeout(pendingOpsTimer);
    pendingOpsTimer = setTimeout(() => {
      pendingOpsTimer = null;
      if (!pendingOps.length) return;
      const batch = pendingOps;
      pendingOps = [];
      socket.emit("ops", { ops: batch });
    }, 35);
  }

  function enqueueAddOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft) return;
    enqueueOps({ type: "add", object: serializeObject(obj) });
  }

  function enqueueUpdateOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft) return;
    enqueueOps({ type: "update", object: serializeObject(obj) });
  }

  function emitUpdateOpImmediate(obj, absolute = false) {
    if (!canEdit || isRemoteApplying || suppressBroadcast) return;
    if (!isSyncableObject(obj) || obj._isDraft) return;
    socket.emit("ops", { ops: [{ type: "update", object: absolute ? serializeAbsoluteObject(obj) : serializeObject(obj) }] });
  }

  function enqueueRemoveOp(obj) {
    if (!isSyncableObject(obj) || obj._isDraft || !obj.obj_id) return;
    enqueueOps({ type: "remove", obj_id: obj.obj_id });
  }

  function enqueueSelectionUpdates() {
    const objects = getSelectionObjects();
    if (!objects.length) return;
    enqueueOps(
      objects
        .filter((obj) => isSyncableObject(obj))
        .map((obj) => ({ type: "update", object: serializeObject(obj) })),
    );
  }

  function enqueueTextUpdateDebounced(obj) {
    if (!obj) return;
    if (pendingTextSyncTimer) clearTimeout(pendingTextSyncTimer);
    pendingTextSyncTimer = setTimeout(() => {
      pendingTextSyncTimer = null;
      enqueueUpdateOp(obj);
    }, 60);
  }

  function applyCanvasState(canvasJson) {
    const selectedIds = captureActiveSelectionIds();
    isRemoteApplying = true;
    Promise.resolve(fabricCanvas.loadFromJSON(canvasJson)).then(() => {
      isRemoteApplying = false;
      ensureCanvasTransparentBackground();
      syncRoundedImages();
      syncObjectInteractivity();
      restoreSelectionByIds(selectedIds);
      updateStylePanelVisibility();
      updateRotateButtonState();
      drawMiniMap();
    }).catch(() => {
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
    const type = String(op.type || "").toLowerCase();
    if (type === "remove") {
      const target = findObjectById(op.obj_id);
      if (target) fabricCanvas.remove(target);
      return;
    }
    if (type === "add" || type === "update") {
      const objectJson = op.object;
      if (!objectJson || typeof objectJson !== "object") return;
      let enlivened = null;
      try {
        enlivened = await enlivenOne(objectJson);
      } catch (_) {
        enlivened = null;
      }
      if (!enlivened) return;
      ensureObjMeta(enlivened);
      const existing = findObjectById(objectJson.obj_id);
      if (existing) {
        fabricCanvas.remove(existing);
      }
      fabricCanvas.add(enlivened);
      if (type === "update") {
        const activeIds = new Set(captureActiveSelectionIds());
        if (activeIds.has(objectJson.obj_id)) {
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
          } catch (_) {
            // skip bad op and continue applying the rest
          }
        }
      } finally {
        isRemoteApplying = false;
      }
      syncRoundedImages();
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
    miniCtx.fillStyle = "#fbfbff";
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

    miniCtx.fillStyle = "rgba(124,58,237,0.5)";
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

    miniCtx.fillStyle = "rgba(13,110,253,0.10)";
    miniCtx.fillRect(vpX, vpY, vpW, vpH);
    miniCtx.strokeStyle = "#0d6efd";
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

  function addImageAtWorldPoint(dataUrl, worldX, worldY) {
    fabric.Image.fromURL(dataUrl, { crossOrigin: "anonymous" }).then((img) => {
      const maxW = 640;
      const scale = img.width > maxW ? maxW / img.width : 1;
      img.scale(scale);
      img.set({
        left: worldX - img.getScaledWidth() / 2,
        top: worldY - img.getScaledHeight() / 2,
      });
      ensureObjMeta(img);
      fabricCanvas.add(img);
      enqueueAddOp(img);
      fabricCanvas.setActiveObject(img);
      updateStylePanelVisibility();
    });
  }

  function worldCenterOfViewport() {
    return worldFromScreen(fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2);
  }

  function deleteActiveObjects() {
    const active = fabricCanvas.getActiveObject();
    if (!active) return;
    const isMulti =
      (typeof active.type === "string" && active.type.toLowerCase() === "activeselection")
      || (typeof active.getObjects === "function" && Array.isArray(active.getObjects()) && active.getObjects().length > 0);
    if (isMulti) {
      const objects = typeof active.getObjects === "function" ? [...active.getObjects()] : [];
      fabricCanvas.discardActiveObject();
      objects.forEach((o) => fabricCanvas.remove(o));
    } else {
      fabricCanvas.discardActiveObject();
      fabricCanvas.remove(active);
    }
    fabricCanvas.requestRenderAll();
    updateStylePanelVisibility();
  }

  function createShape(type, start, end) {
    const x = Math.min(start.x, end.x);
    const y = Math.min(start.y, end.y);
    const width = Math.max(1, Math.abs(end.x - start.x));
    const height = Math.max(1, Math.abs(end.y - start.y));
    const strokeW = Number(strokeWidthEl.value);

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
      const tri = createRoundedPolygonShape(type, x, y, width, height, strokeW, 0);
      ensureObjMeta(tri);
      return tri;
    }

    const diamond = createRoundedPolygonShape("diamond", x, y, width, height, strokeW, 0);
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
    const panByModifier = evt.button === 1 || (evt.button === 0 && (evt.ctrlKey || evt.metaKey));
    const panByTool = currentTool === "hand" && evt.button === 0;

    if (panByModifier || panByTool) {
      panMode = true;
      panLast = { x: evt.clientX, y: evt.clientY };
      fabricCanvas.defaultCursor = "grabbing";
      return;
    }

    if (!canEdit) return;

    if (currentTool === "pencil" || currentTool === "shape" || currentTool === "text" || currentTool === "eraser") {
      hidePanels();
    }

    const p = fabricCanvas.getScenePoint(evt);

    if (currentTool === "text") {
      const active = fabricCanvas.getActiveObject();
      if (active && active.type === "i-text" && active.isEditing) return;
      if (skipNextTextCreate) {
        skipNextTextCreate = false;
        return;
      }

      const text = new fabric.IText("", {
        left: p.x,
        top: p.y,
        fill: currentColor,
        fontSize: Number(textSizeEl ? textSizeEl.value : 24),
        fontFamily: "Montserrat, sans-serif",
        fontWeight: "500",
      });
      ensureObjMeta(text);
      fabricCanvas.add(text);
      enqueueAddOp(text);
      fabricCanvas.setActiveObject(text);
      text.enterEditing();
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
    const world = worldFromScreen(evt.offsetX, evt.offsetY);

    if (panMode) {
      const v = fabricCanvas.viewportTransform;
      const dx = evt.clientX - panLast.x;
      const dy = evt.clientY - panLast.y;
      v[4] += dx;
      v[5] += dy;
      panLast = { x: evt.clientX, y: evt.clientY };
      fabricCanvas.requestRenderAll();
      renderRemoteCursors();
      drawMiniMap();
      updateGridPosition();
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
      drawingShape = null;
      drawingStart = null;
    }

  }

  fabricCanvas.on("mouse:down", onMouseDown);
  fabricCanvas.on("mouse:move", onMouseMove);
  fabricCanvas.on("mouse:up", onMouseUp);

  fabricCanvas.on("mouse:wheel", (opt) => {
    const factor = opt.e.deltaY > 0 ? 0.94 : 1.065;
    zoomByFactor(factor, opt.e.offsetX, opt.e.offsetY);
    opt.e.preventDefault();
    opt.e.stopPropagation();
  });

  fabricCanvas.on("text:editing:exited", () => {
    skipNextTextCreate = true;
    const active = fabricCanvas.getActiveObject();
    if (active && active.type === "i-text") enqueueUpdateOp(active);
  });

  fabricCanvas.on("text:changed", (e) => {
    const target = e && e.target;
    if (target && target.type === "i-text") enqueueTextUpdateDebounced(target);
  });

  fabricCanvas.on("path:created", (e) => {
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
    if (e.target) ensureObjMeta(e.target);
    drawMiniMap();
  });

  fabricCanvas.on("object:modified", (e) => {
    const target = e && e.target;
    if (target) {
      const applyOne = (obj) => {
        if (obj && obj.type === "image") applyImageCornerRadius(obj, getObjectCornerRadius(obj));
      };
      if (isMultiSelectionObject(target) && typeof target.getObjects === "function") {
        target.getObjects().forEach((obj) => {
          if (!obj) return;
          applyOne(obj);
          obj.setCoords();
          emitUpdateOpImmediate(obj, true);
        });
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
    applySelectionStyles();
    updateStylePanelVisibility();
    updateRotateButtonState();
  });

  fabricCanvas.on("selection:updated", () => {
    applySelectionStyles();
    updateStylePanelVisibility();
    updateRotateButtonState();
  });

  fabricCanvas.on("selection:cleared", () => {
    if (stylePanel) stylePanel.classList.remove("active");
    updateUndoRedoDockVisibility();
    updateRotateButtonState();
  });

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
        if (currentTool === "shape" && shapePanel && shapePanel.classList.contains("active")) {
          hidePanels();
          return;
        }
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
    hiddenImageInput.click();
  });

  hiddenImageInput.addEventListener("change", () => {
    const file = hiddenImageInput.files && hiddenImageInput.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const center = worldCenterOfViewport();
      addImageAtWorldPoint(reader.result, center.x, center.y);
    };
    reader.readAsDataURL(file);
    hiddenImageInput.value = "";
  });

  boardWrap.addEventListener("dragover", (e) => e.preventDefault());
  boardWrap.addEventListener("drop", (e) => {
    e.preventDefault();
    if (!canEdit) return;
    const file = [...(e.dataTransfer?.files || [])].find((f) => f.type.startsWith("image/"));
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const p = activeDropWorldPoint(e.clientX, e.clientY);
      addImageAtWorldPoint(reader.result, p.x, p.y);
    };
    reader.readAsDataURL(file);
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

  undoBtn.addEventListener("click", () => socket.emit("undo"));
  redoBtn.addEventListener("click", () => socket.emit("redo"));
  if (rotateBtn) {
    rotateBtn.addEventListener("click", () => {
      const active = fabricCanvas.getActiveObject();
      if (!active || !canEdit) return;
      active.rotate((active.angle || 0) + 15);
      active.setCoords();
      fabricCanvas.requestRenderAll();
      enqueueSelectionUpdates();
    });
  }
  clearBtn.addEventListener("click", () => socket.emit("clear"));
  bgBtn.addEventListener("click", toggleBackground);
  if (mobileBgBtn) mobileBgBtn.addEventListener("click", toggleBackground);

  if (mobileImageBtn) {
    mobileImageBtn.addEventListener("click", () => {
      if (!canEdit) return;
      hiddenImageInput.click();
    });
  }
  if (mobileRotateBtn) {
    mobileRotateBtn.addEventListener("click", () => {
      if (rotateBtn) rotateBtn.click();
    });
  }
  if (mobileClearBtn) {
    mobileClearBtn.addEventListener("click", () => {
      if (clearBtn) clearBtn.click();
    });
  }
  if (mobileMoreBtn && mobileMorePanel) {
    mobileMoreBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      updateRotateButtonState();
      const active = mobileMorePanel.classList.contains("active");
      hidePanels();
      if (!active) {
        mobileMorePanel.classList.add("active");
        placePanelNear(mobileMorePanel, mobileMoreBtn);
      }
    });
  }

  zoomOutBtn.addEventListener("click", () => zoomByFactor(0.9, fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2));
  zoomInBtn.addEventListener("click", () => zoomByFactor(1.1, fabricCanvas.getWidth() / 2, fabricCanvas.getHeight() / 2));
  zoomCenterBtn.addEventListener("click", resetView);

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
    hidePanels();
  });

  document.addEventListener("click", (e) => {
    const inside = e.target.closest("#toolbar") || e.target.closest(".floating-panel");
    if (!inside) hidePanels();
  });

  boardWrap.addEventListener("touchstart", (e) => {
    if (e.touches.length === 2) {
      pinchMode = true;
      const [t1, t2] = e.touches;
      pinchDist = Math.hypot(t2.clientX - t1.clientX, t2.clientY - t1.clientY);
      pinchCenter = { x: (t1.clientX + t2.clientX) / 2, y: (t1.clientY + t2.clientY) / 2 };
    }
  }, { passive: false });

  boardWrap.addEventListener("touchmove", (e) => {
    if (!pinchMode || e.touches.length !== 2) return;
    e.preventDefault();
    const [t1, t2] = e.touches;
    const dist = Math.hypot(t2.clientX - t1.clientX, t2.clientY - t1.clientY);
    const center = { x: (t1.clientX + t2.clientX) / 2, y: (t1.clientY + t2.clientY) / 2 };

    const factor = dist / Math.max(1, pinchDist);
    const rect = boardWrap.getBoundingClientRect();
    zoomByFactor(factor, center.x - rect.left, center.y - rect.top);

    const v = fabricCanvas.viewportTransform;
    v[4] += center.x - pinchCenter.x;
    v[5] += center.y - pinchCenter.y;
    fabricCanvas.requestRenderAll();

    pinchDist = dist;
    pinchCenter = center;
    renderRemoteCursors();
    drawMiniMap();
    updateGridPosition();
  }, { passive: false });

  boardWrap.addEventListener("touchend", (e) => {
    if (e.touches.length < 2) pinchMode = false;
  });

  socket.on("connect", () => {
    // init приходит от сервера после подключения и переподключения
  });

  socket.on("disconnect", () => {
    // reconnection managed by socket.io
  });

  socket.on("init", (msg) => {
    myClientId = msg.client_id || "";
    myUserId = msg.user_id || myUserId;
    myJwtRole = msg.jwt_role || myJwtRole;
    boardRoleCanEdit = msg.role === "owner" || msg.role === "editor";
    canEdit = typeof msg.can_edit === "boolean" ? msg.can_edit : boardRoleCanEdit;
    canClear = !!msg.can_clear;

    clearBtn.style.display = canClear ? "inline-flex" : "none";
    if (mobileClearBtn) mobileClearBtn.style.display = canClear ? "inline-flex" : "none";

    applyEditPermissions();

    applyCanvasState(msg.canvas_json || { version: "6.0.0", objects: [] });
    socket.emit("history_state_request");
  });

  socket.on("board_policy", (msg) => {
    const allowStudentsDraw = !!(msg && msg.allow_students_draw);
    canEdit = allowStudentsDraw ? boardRoleCanEdit : (myJwtRole === "moderator");
    applyEditPermissions();
  });

  socket.on("ops", (msg) => {
    if (msg && Array.isArray(msg.ops)) applyRemoteOps(msg.ops);
  });

  socket.on("update", (msg) => {
    if (msg.canvas_json) applyCanvasState(msg.canvas_json);
  });

  socket.on("clear", (msg) => {
    applyCanvasState(msg.canvas_json || { version: "6.0.0", objects: [] });
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
    console.error(msg?.message || "Socket error");
  });

  setInterval(() => {
    const cutoff = Date.now() - 12000;
    for (const [clientId, item] of remoteCursors.entries()) {
      if (item.ts < cutoff) removeRemoteCursor(clientId);
    }
  }, 3000);

  setupObjectStyles();
  buildPalette();
  setShapeType("rect");
  setBackground("grid");
  resizeCanvas(true);
  ensureCanvasTransparentBackground();
  updateUndoRedoDockVisibility();
  setTool("select");
  ensureCursorAnimation();
})();
