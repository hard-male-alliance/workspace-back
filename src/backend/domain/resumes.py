"""@brief API v2 Resume 语义中间表示与聚合 / API v2 Resume SIR and aggregate."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import StrEnum
from hashlib import sha256
from typing import NewType
from urllib.parse import urlsplit

from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.resources import ResourceRef

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
"""@brief JSON 值的递归类型 / Recursive JSON value type."""

ResumeId = NewType("ResumeId", str)
"""@brief Resume 不透明标识 / Opaque Resume identifier."""

ResumeOperationId = NewType("ResumeOperationId", str)
"""@brief Resume operation 不透明标识 / Opaque Resume operation identifier."""

ResumeBatchId = NewType("ResumeBatchId", str)
"""@brief 客户端离线批次标识 / Client offline-batch identifier."""

ResumeProposalId = NewType("ResumeProposalId", str)
"""@brief Resume proposal 不透明标识 / Opaque Resume proposal identifier."""

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief v2 不透明标识语法 / API v2 opaque-ID grammar."""

_LOCALE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
"""@brief v2 locale 语法 / API v2 locale grammar."""

_FIELD_PART = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
"""@brief operation field path 语法 / Operation field-path grammar."""


class ResumeDomainError(ValueError):
    """@brief 可稳定映射为 API problem 的 Resume 领域错误 / Stable Resume domain error.

    @param code 稳定错误码 / Stable error code.
    @param detail 可公开错误说明 / Public-safe detail.
    """

    code: str
    """@brief 稳定错误码 / Stable error code."""

    detail: str
    """@brief 可公开错误说明 / Public-safe error detail."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化 Resume 错误 / Initialize a Resume error.

        @param code 稳定错误码 / Stable error code.
        @param detail 可公开说明 / Public-safe detail.
        """
        super().__init__(detail)
        self.code = code
        self.detail = detail


class ResumeRevisionConflict(ResumeDomainError):
    """@brief 客户端基础 revision 无法安全应用 / Client base revision cannot be applied safely.

    @param current_revision 服务端当前 revision / Server-side current revision.
    @param conflicts 结构化冲突 / Structured conflicts.
    """

    current_revision: int
    """@brief 当前 revision / Current revision."""

    conflicts: tuple[ResumeConflict, ...]
    """@brief 结构化冲突 / Structured conflicts."""

    def __init__(self, current_revision: int, conflicts: Sequence[ResumeConflict] = ()) -> None:
        """@brief 初始化 revision 冲突 / Initialize a revision conflict.

        @param current_revision 当前 revision / Current revision.
        @param conflicts 可选的 operation 冲突 / Optional operation conflicts.
        """
        super().__init__("resume.revision_conflict", "resume revision is stale")
        self.current_revision = current_revision
        self.conflicts = tuple(conflicts)


class ResumeBatchKeyReused(ResumeDomainError):
    """@brief 批次标识或 operation 标识被不同输入重用 / Batch or operation ID was reused."""

    def __init__(self, *, operation: bool = False) -> None:
        """@brief 创建稳定重用错误 / Create a stable reuse error.

        @param operation 是否为 operation ID 冲突 / Whether an operation ID conflicted.
        """
        code = "resume.operation_id_reused" if operation else "idempotency.key_reused"
        detail = "operation id was reused with different input" if operation else "batch id was reused with different input"
        super().__init__(code, detail)


class TextMarkKind(StrEnum):
    """@brief RichText mark 种类 / RichText mark kinds."""

    STRONG = "strong"
    EMPHASIS = "emphasis"
    LINK = "link"


class ContactKind(StrEnum):
    """@brief 简历联系方式种类 / Resume contact-method kinds."""

    EMAIL = "email"
    PHONE = "phone"
    WEBSITE = "website"
    LINKEDIN = "linkedin"
    GITHUB = "github"
    PORTFOLIO = "portfolio"
    LOCATION = "location"
    OTHER = "other"
    CUSTOM = "custom"


class ResumeItemKind(StrEnum):
    """@brief Resume item 种类 / Resume item kinds."""

    EXPERIENCE = "experience"
    EDUCATION = "education"
    PROJECT = "project"
    SKILL_GROUP = "skill_group"
    PUBLICATION = "publication"
    AWARD = "award"
    CERTIFICATION = "certification"
    LANGUAGE = "language"
    VOLUNTEER = "volunteer"
    CUSTOM = "custom"


class ResumeSectionKind(StrEnum):
    """@brief Resume section 种类 / Resume section kinds."""

    EXPERIENCE = "experience"
    EDUCATION = "education"
    PROJECTS = "projects"
    SKILLS = "skills"
    PUBLICATIONS = "publications"
    AWARDS = "awards"
    CERTIFICATIONS = "certifications"
    LANGUAGES = "languages"
    VOLUNTEER = "volunteer"
    CUSTOM = "custom"


class MeasurementUnit(StrEnum):
    """@brief 模板尺寸单位 / Template measurement units."""

    PT = "pt"
    MM = "mm"
    CM = "cm"
    IN = "in"
    PX = "px"
    EM = "em"
    PERCENT = "percent"


class PageSize(StrEnum):
    """@brief 纸张尺寸 / Page sizes."""

    A4 = "A4"
    LETTER = "LETTER"
    LEGAL = "LEGAL"
    CUSTOM = "CUSTOM"


class PageOrientation(StrEnum):
    """@brief 纸张方向 / Page orientations."""

    PORTRAIT = "portrait"
    LANDSCAPE = "landscape"


class ColorSpace(StrEnum):
    """@brief 契约颜色空间 / Contract color spaces."""

    SRGB_HEX = "srgb_hex"
    RGBA = "rgba"


class ConflictStrategy(StrEnum):
    """@brief operation batch 冲突策略 / Operation-batch conflict strategies."""

    REJECT = "reject"
    REBASE_IF_SAFE = "rebase_if_safe"


class RenderHint(StrEnum):
    """@brief operation batch 渲染意图 / Operation-batch render hints."""

    NONE = "none"
    PREVIEW = "preview"
    FINAL = "final"


class EntityKind(StrEnum):
    """@brief 可移除或移动的 SIR entity 种类 / Movable or removable SIR entity kinds."""

    SECTION = "section"
    ITEM = "item"


@dataclass(frozen=True, slots=True)
class PartialDate:
    """@brief 经验证的部分日期 / Validated partial calendar date.

    @param value YYYY、YYYY-MM 或 YYYY-MM-DD / YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    value: str

    def __post_init__(self) -> None:
        """@brief 校验真实日历日期 / Validate a real calendar date.

        @raise ResumeDomainError 日期格式或日历值无效时抛出 / Raised for invalid dates.
        """
        parts = self.value.split("-")
        if len(parts) not in {1, 2, 3} or len(parts[0]) != 4 or not all(part.isdigit() for part in parts):
            raise ResumeDomainError("resume.invalid_date", "partial date is invalid")
        year = int(parts[0])
        month = int(parts[1]) if len(parts) >= 2 else 1
        day = int(parts[2]) if len(parts) == 3 else 1
        try:
            date(year, month, day)
        except ValueError as error:
            raise ResumeDomainError("resume.invalid_date", "partial date is invalid") from error

    def lower_bound(self) -> date:
        """@brief 返回部分日期的最早日 / Return the earliest represented day.

        @return 最早日历日 / Earliest calendar day.
        """
        parts = [int(part) for part in self.value.split("-")]
        return date(parts[0], parts[1] if len(parts) >= 2 else 1, parts[2] if len(parts) == 3 else 1)

    def upper_bound(self) -> date:
        """@brief 返回部分日期的最晚日 / Return the latest represented day.

        @return 最晚日历日 / Latest calendar day.
        """
        parts = [int(part) for part in self.value.split("-")]
        if len(parts) == 3:
            return date(parts[0], parts[1], parts[2])
        if len(parts) == 2:
            next_month = date(parts[0] + (parts[1] == 12), 1 if parts[1] == 12 else parts[1] + 1, 1)
            return date.fromordinal(next_month.toordinal() - 1)
        return date(parts[0], 12, 31)


@dataclass(frozen=True, slots=True)
class DateRange:
    """@brief 允许部分精度的日期区间 / Date range supporting partial precision.

    @param start 可缺省开始日期 / Optional start date.
    @param end 可缺省结束日期；None 与 present 含义不同 / Optional end; None differs from present.
    @param present 结束是否为 present / Whether the end is present.
    """

    start: PartialDate | None
    end: PartialDate | None
    present: bool = False

    def __post_init__(self) -> None:
        """@brief 拒绝倒序或模糊的日期区间 / Reject reversed or ambiguous ranges.

        @raise ResumeDomainError 结束同时表示日期和 present，或区间倒序时抛出 / Raised for invalid ranges.
        """
        if self.present and self.end is not None:
            raise ResumeDomainError("resume.invalid_date_range", "date range end cannot be both a date and present")
        if self.start is not None and self.end is not None and self.start.lower_bound() > self.end.upper_bound():
            raise ResumeDomainError("resume.invalid_date_range", "date range cannot be reversed")


@dataclass(frozen=True, slots=True)
class TextMark:
    """@brief RichText 半开区间 mark / RichText half-open interval mark.

    @param start 起始 Unicode code-point index / Starting Unicode code-point index.
    @param end 结束 Unicode code-point index / Ending Unicode code-point index.
    @param kind mark 种类 / Mark kind.
    @param href 仅 link mark 携带的安全 URL / Safe URL carried only by link marks.
    """

    start: int
    end: int
    kind: TextMarkKind
    href: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验 mark 自身边界 / Validate intrinsic mark bounds.

        @raise ResumeDomainError mark 区间或 link 无效时抛出 / Raised for invalid bounds or links.
        """
        if self.start < 0 or self.end <= self.start:
            raise ResumeDomainError("resume.invalid_text_mark", "text mark must satisfy start < end")
        if self.kind is TextMarkKind.LINK:
            if self.href is None or not _is_safe_link(self.href):
                raise ResumeDomainError("resume.invalid_text_mark", "link mark requires a safe URL")
        elif self.href is not None:
            raise ResumeDomainError("resume.invalid_text_mark", "only link marks may carry href")


@dataclass(frozen=True, slots=True)
class RichText:
    """@brief 结构化富文本 / Structured rich text.

    @param text 纯文本 / Plain text.
    @param marks 稳定顺序 mark / Stably ordered marks.
    """

    text: str
    marks: tuple[TextMark, ...] = ()

    def __post_init__(self) -> None:
        """@brief 校验 mark 边界与交叉 / Validate mark bounds and intersections.

        @raise ResumeDomainError 超长、越界、重复或交叉 mark 时抛出 / Raised for invalid marks.
        """
        if len(self.text) > 20_000 or len(self.marks) > 1_000:
            raise ResumeDomainError("resume.invalid_rich_text", "rich text exceeds contract limits")
        seen: set[tuple[int, int, TextMarkKind, str | None]] = set()
        for mark in self.marks:
            if mark.end > len(self.text):
                raise ResumeDomainError("resume.invalid_text_mark", "text mark exceeds text length")
            identity = (mark.start, mark.end, mark.kind, mark.href)
            if identity in seen:
                raise ResumeDomainError("resume.invalid_text_mark", "duplicate text marks are forbidden")
            seen.add(identity)
        for index, left in enumerate(self.marks):
            for right in self.marks[index + 1 :]:
                crossing = left.start < right.start < left.end < right.end or right.start < left.start < right.end < left.end
                overlapping_links = left.kind is TextMarkKind.LINK and right.kind is TextMarkKind.LINK and max(left.start, right.start) < min(left.end, right.end)
                if crossing or overlapping_links:
                    raise ResumeDomainError("resume.invalid_text_mark", "text marks overlap illegally")


@dataclass(frozen=True, slots=True)
class ContactMethod:
    """@brief 简历联系方式 / Resume contact method."""

    id: str
    kind: ContactKind
    label: str | None
    value: str
    url: str | None

    def __post_init__(self) -> None:
        """@brief 校验联系方式 / Validate a contact method.

        @raise ResumeDomainError 字段超界或 URL 不安全时抛出 / Raised for invalid fields.
        """
        _require_id(self.id, "contact id")
        _optional_max(self.label, 80, "contact label")
        _required_text(self.value, 500, "contact value")
        if self.url is not None and not _is_safe_link(self.url):
            raise ResumeDomainError("resume.invalid_contact", "contact URL is unsafe")


@dataclass(frozen=True, slots=True)
class ResumeProfile:
    """@brief Resume 个人资料 / Resume profile."""

    full_name: str
    headline: str | None = None
    summary: RichText | None = None
    contacts: tuple[ContactMethod, ...] = ()

    def __post_init__(self) -> None:
        """@brief 校验 profile 长度与联系 ID / Validate profile lengths and contact IDs.

        @raise ResumeDomainError profile 不符合契约时抛出 / Raised for invalid profiles.
        """
        _required_text(self.full_name, 200, "profile full name")
        _optional_max(self.headline, 300, "profile headline")
        if len(self.contacts) > 30 or len({contact.id for contact in self.contacts}) != len(self.contacts):
            raise ResumeDomainError("resume.invalid_profile", "profile contact IDs must be unique and within limits")


@dataclass(frozen=True, slots=True)
class ResumeItem:
    """@brief 规范化 Resume item / Normalized Resume item."""

    id: str
    kind: ResumeItemKind
    title: str | None = None
    subtitle: str | None = None
    organization: str | None = None
    location: str | None = None
    date_range: DateRange | None = None
    summary: RichText | None = None
    highlights: tuple[RichText, ...] = ()
    skills: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    visible: bool = True
    url: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验 item 契约不变量 / Validate item contract invariants.

        @raise ResumeDomainError item 无效时抛出 / Raised for invalid items.
        """
        _require_id(self.id, "item id")
        for value, label in ((self.title, "title"), (self.subtitle, "subtitle"), (self.organization, "organization"), (self.location, "location")):
            _optional_max(value, 300, f"item {label}")
        if len(self.highlights) > 100 or len(self.skills) > 200 or len(self.tags) > 100:
            raise ResumeDomainError("resume.invalid_item", "item collection exceeds contract limits")
        if len(set(self.tags)) != len(self.tags):
            raise ResumeDomainError("resume.invalid_item", "item tags must be unique")
        for value in (*self.skills, *self.tags):
            _required_text(value, 100, "item skill or tag")
        if self.url is not None and not _is_safe_link(self.url):
            raise ResumeDomainError("resume.invalid_item", "item URL is unsafe")


@dataclass(frozen=True, slots=True)
class ResumeSection:
    """@brief Resume section / Resume section."""

    id: str
    kind: ResumeSectionKind
    title: str
    visible: bool = True
    content: RichText | None = None
    items: tuple[ResumeItem, ...] = ()

    def __post_init__(self) -> None:
        """@brief 校验 section 字段与 item ID / Validate section fields and item IDs.

        @raise ResumeDomainError section 无效时抛出 / Raised for invalid sections.
        """
        _require_id(self.id, "section id")
        _required_text(self.title, 120, "section title")
        if len(self.items) > 1_000 or len({item.id for item in self.items}) != len(self.items):
            raise ResumeDomainError("resume.invalid_section", "section item IDs must be unique and within limits")


@dataclass(frozen=True, slots=True)
class TemplateRef:
    """@brief 不可变模板版本引用 / Immutable template-version reference."""

    template_id: str
    version: str

    def __post_init__(self) -> None:
        """@brief 校验模板引用 / Validate a template reference.

        @raise ResumeDomainError 标识或版本无效时抛出 / Raised for invalid references.
        """
        _require_id(self.template_id, "template id")
        _required_text(self.version, 80, "template version")


@dataclass(frozen=True, slots=True)
class Measurement:
    """@brief 带单位的模板测量 / Unit-bearing template measurement."""

    value: float
    unit: MeasurementUnit

    def __post_init__(self) -> None:
        """@brief 拒绝非有限测量 / Reject non-finite measurements.

        @raise ResumeDomainError 测量非有限时抛出 / Raised for non-finite values.
        """
        if not math.isfinite(self.value):
            raise ResumeDomainError("resume.invalid_style", "measurement must be finite")


@dataclass(frozen=True, slots=True)
class PageInsets:
    """@brief 页面四边距 / Four page insets."""

    top: Measurement
    right: Measurement
    bottom: Measurement
    left: Measurement


@dataclass(frozen=True, slots=True)
class ResumePageIntent:
    """@brief Resume 页面意图 / Resume page intent."""

    size: PageSize
    custom_width: Measurement | None
    custom_height: Measurement | None
    orientation: PageOrientation
    margins: PageInsets
    max_pages: int | None
    show_page_numbers: bool

    def __post_init__(self) -> None:
        """@brief 校验自定义尺寸与页数 / Validate custom size and page count.

        @raise ResumeDomainError 页面意图无效时抛出 / Raised for invalid page intent.
        """
        custom = self.size is PageSize.CUSTOM
        if custom != (self.custom_width is not None and self.custom_height is not None):
            raise ResumeDomainError("resume.invalid_style", "custom page size requires width and height only for CUSTOM")
        if self.max_pages is not None and not 1 <= self.max_pages <= 100:
            raise ResumeDomainError("resume.invalid_style", "max_pages is outside contract limits")


@dataclass(frozen=True, slots=True)
class TypographyIntent:
    """@brief Resume 排版意图 / Resume typography intent."""

    font_family_token: str
    base_size_pt: float
    line_height: float
    heading_scale: float
    letter_spacing_em: float

    def __post_init__(self) -> None:
        """@brief 校验排版参数 / Validate typography parameters.

        @raise ResumeDomainError 参数超出契约范围时抛出 / Raised outside contract ranges.
        """
        _required_text(self.font_family_token, 120, "font token")
        values = (self.base_size_pt, self.line_height, self.heading_scale, self.letter_spacing_em)
        if not all(math.isfinite(value) for value in values):
            raise ResumeDomainError("resume.invalid_style", "typography values must be finite")
        if not 5 <= self.base_size_pt <= 72 or not 0.5 <= self.line_height <= 5 or not 0.5 <= self.heading_scale <= 5 or not -1 <= self.letter_spacing_em <= 2:
            raise ResumeDomainError("resume.invalid_style", "typography value is outside contract limits")


@dataclass(frozen=True, slots=True)
class ColorValue:
    """@brief 经命名颜色值 / Named color value."""

    space: ColorSpace
    value: str

    def __post_init__(self) -> None:
        """@brief 校验颜色字符串 / Validate the color string.

        @raise ResumeDomainError 颜色无效时抛出 / Raised for invalid colors.
        """
        _required_text(self.value, 80, "color")
        if self.space is ColorSpace.SRGB_HEX and re.fullmatch(r"#[0-9A-Fa-f]{6}(?:[0-9A-Fa-f]{2})?", self.value) is None:
            raise ResumeDomainError("resume.invalid_style", "sRGB color must be hexadecimal")


@dataclass(frozen=True, slots=True)
class PaletteIntent:
    """@brief Resume 色板意图 / Resume palette intent."""

    primary: ColorValue
    secondary: ColorValue
    text: ColorValue
    muted_text: ColorValue
    background: ColorValue


@dataclass(frozen=True, slots=True)
class SectionLayoutIntent:
    """@brief 单个 section 的布局意图 / Layout intent for one section."""

    section_id: str
    zone: str
    keep_together: bool
    page_break_before: bool
    compactness: float
    heading_style_token: str | None

    def __post_init__(self) -> None:
        """@brief 校验 section 布局 / Validate section layout intent.

        @raise ResumeDomainError 布局参数无效时抛出 / Raised for invalid layout values.
        """
        _require_id(self.section_id, "layout section id")
        _required_text(self.zone, 80, "layout zone")
        _optional_max(self.heading_style_token, 120, "heading style token")
        if not math.isfinite(self.compactness) or not 0 <= self.compactness <= 1:
            raise ResumeDomainError("resume.invalid_style", "section compactness is outside contract limits")


@dataclass(frozen=True, slots=True)
class ResumeStyleIntent:
    """@brief 与渲染器解耦的 Resume 样式意图 / Renderer-independent Resume style intent."""

    page: ResumePageIntent
    typography: TypographyIntent
    palette: PaletteIntent
    density: float
    date_format_token: str
    bullet_style_token: str
    section_layout: tuple[SectionLayoutIntent, ...] = ()
    template_settings: Mapping[str, JsonValue] = field(default_factory=dict)
    extensions: Mapping[str, JsonValue] = field(default_factory=dict)
    style_contract_version: str = "1.0"

    def __post_init__(self) -> None:
        """@brief 校验样式意图全局限制 / Validate global style-intent limits.

        @raise ResumeDomainError 样式无效时抛出 / Raised for invalid style intent.
        """
        if self.style_contract_version != "1.0":
            raise ResumeDomainError("resume.invalid_style", "unsupported style contract version")
        if not math.isfinite(self.density) or not 0 <= self.density <= 1:
            raise ResumeDomainError("resume.invalid_style", "density is outside contract limits")
        _required_text(self.date_format_token, 120, "date format token")
        _required_text(self.bullet_style_token, 120, "bullet style token")
        if len(self.section_layout) > 100 or len(self.template_settings) > 100 or len(self.extensions) > 32:
            raise ResumeDomainError("resume.invalid_style", "style collection exceeds contract limits")
        if len({layout.section_id for layout in self.section_layout}) != len(self.section_layout):
            raise ResumeDomainError("resume.invalid_style", "section layout IDs must be unique")


class TemplateSettingValueType(StrEnum):
    """@brief 模板 setting 值类型 / Template-setting value types."""

    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    STRING = "string"
    CHOICE = "choice"
    COLOR = "color"
    MEASUREMENT = "measurement"


@dataclass(frozen=True, slots=True)
class TemplateSettingRule:
    """@brief 原子化 setting 校验规则 / Atomic template-setting validation rule."""

    key: str
    value_type: TemplateSettingValueType
    default: JsonValue
    minimum: float | None = None
    maximum: float | None = None
    choices: tuple[JsonValue, ...] = ()
    visible_when: tuple[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        """@brief 校验 manifest setting 声明自身 / Validate the manifest setting declaration itself.

        @raise ResumeDomainError key、范围或 choice 声明矛盾时抛出 / Raised for malformed declarations.
        """
        if re.fullmatch(r"^[a-z][a-z0-9_.-]{1,80}$", self.key) is None:
            raise ResumeDomainError(
                "resume.template_invalid",
                "template setting key is invalid",
            )
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            raise ResumeDomainError(
                "resume.template_invalid",
                "template setting range is reversed",
            )
        if self.value_type is TemplateSettingValueType.CHOICE and not self.choices:
            raise ResumeDomainError(
                "resume.template_invalid",
                "choice setting requires choices",
            )
        if self.visible_when is not None and not self.visible_when[0]:
            raise ResumeDomainError(
                "resume.template_invalid",
                "setting visibility dependency is invalid",
            )
        if self.visible_when is None:
            self.validate(self.default, {self.key: self.default})

    def validate(self, value: JsonValue, settings: Mapping[str, JsonValue]) -> None:
        """@brief 校验 setting 类型、取值和可见性 / Validate setting type, range, choice, and visibility.

        @param value 待校验 JSON 值 / JSON value to validate.
        @param settings 完整 setting 集合 / Complete settings collection.
        @raise ResumeDomainError setting 不兼容时抛出 / Raised for incompatible settings.
        """
        if self.visible_when is not None:
            dependency, expected = self.visible_when
            if _canonical_json(settings.get(dependency)) != _canonical_json(expected):
                raise ResumeDomainError("resume.template_incompatible", f"setting {self.key} is not visible")
        valid = _setting_type_matches(self.value_type, value)
        if not valid:
            raise ResumeDomainError("resume.template_incompatible", f"setting {self.key} has the wrong type")
        if self.choices and all(_canonical_json(value) != _canonical_json(choice) for choice in self.choices):
            raise ResumeDomainError("resume.template_incompatible", f"setting {self.key} is not an allowed choice")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = float(value)
            if (self.minimum is not None and numeric < self.minimum) or (
                self.maximum is not None and numeric > self.maximum
            ):
                raise ResumeDomainError("resume.template_incompatible", f"setting {self.key} is outside its range")


@dataclass(frozen=True, slots=True)
class TemplateZonePolicy:
    """@brief 模板 zone 兼容策略 / Template-zone compatibility policy."""

    id: str
    accepted_section_kinds: frozenset[ResumeSectionKind]
    max_sections: int | None = None


@dataclass(frozen=True, slots=True)
class TemplatePolicy:
    """@brief 从不可变 manifest 投影的原子兼容策略 / Atomic compatibility policy projected from a manifest."""

    ref: TemplateRef
    supported_locales: frozenset[str]
    supported_page_sizes: frozenset[PageSize]
    supported_output_formats: frozenset[str]
    supported_section_kinds: frozenset[ResumeSectionKind]
    zones: tuple[TemplateZonePolicy, ...]
    font_family_tokens: frozenset[str]
    date_format_tokens: frozenset[str]
    bullet_style_tokens: frozenset[str]
    settings: tuple[TemplateSettingRule, ...] = ()
    supports_custom_sections: bool = True

    def __post_init__(self) -> None:
        """@brief 校验从 manifest 投影的策略完整性 / Validate policy completeness projected from a manifest.

        @raise ResumeDomainError manifest 能力为空、重复或自相矛盾时抛出 / Raised for empty or contradictory policies.
        """
        required_sets = (
            self.supported_locales,
            self.supported_page_sizes,
            self.supported_output_formats,
            self.font_family_tokens,
            self.date_format_tokens,
            self.bullet_style_tokens,
        )
        if any(not values for values in required_sets) or not self.zones:
            raise ResumeDomainError(
                "resume.template_invalid",
                "template policy lacks required capabilities",
            )
        zone_ids = [zone.id for zone in self.zones]
        setting_keys = [setting.key for setting in self.settings]
        if len(set(zone_ids)) != len(zone_ids) or len(set(setting_keys)) != len(setting_keys):
            raise ResumeDomainError(
                "resume.template_invalid",
                "template zone and setting keys must be unique",
            )
        known_settings = set(setting_keys)
        if any(
            rule.visible_when is not None
            and rule.visible_when[0] not in known_settings
            for rule in self.settings
        ):
            raise ResumeDomainError(
                "resume.template_invalid",
                "template setting visibility dependency is unknown",
            )

    def validate(self, document: ResumeDocument, *, output_formats: Sequence[str] = ()) -> None:
        """@brief 原子校验 Resume 与模板兼容性 / Atomically validate Resume-template compatibility.

        @param document 候选 Resume SIR / Candidate Resume SIR.
        @param output_formats 本次要渲染的格式 / Formats requested for this render.
        @raise ResumeDomainError 任一模板约束不满足时抛出 / Raised for any incompatibility.
        """
        if document.template != self.ref:
            raise ResumeDomainError("resume.template_incompatible", "template policy does not match document reference")
        if document.locale not in self.supported_locales or document.style.page.size not in self.supported_page_sizes:
            raise ResumeDomainError("resume.template_incompatible", "locale or page size is unsupported")
        if any(output_format not in self.supported_output_formats for output_format in output_formats):
            raise ResumeDomainError("resume.template_incompatible", "output format is unsupported")
        if any(section.kind not in self.supported_section_kinds for section in document.sections):
            raise ResumeDomainError("resume.template_incompatible", "section kind is unsupported")
        if not self.supports_custom_sections and any(section.kind is ResumeSectionKind.CUSTOM for section in document.sections):
            raise ResumeDomainError("resume.template_incompatible", "custom sections are unsupported")
        style = document.style
        if style.typography.font_family_token not in self.font_family_tokens or style.date_format_token not in self.date_format_tokens or style.bullet_style_token not in self.bullet_style_tokens:
            raise ResumeDomainError("resume.template_incompatible", "style token is unsupported")
        known_settings = {rule.key: rule for rule in self.settings}
        if unknown := set(style.template_settings) - set(known_settings):
            raise ResumeDomainError("resume.template_incompatible", f"unknown template settings: {sorted(unknown)}")
        for key, value in style.template_settings.items():
            known_settings[key].validate(value, style.template_settings)
        self._validate_zones(document)

    def _validate_zones(self, document: ResumeDocument) -> None:
        """@brief 校验 section 与 template zone 的对应 / Validate section-to-zone assignments.

        @param document 候选 Resume SIR / Candidate Resume SIR.
        @raise ResumeDomainError zone 或容量无效时抛出 / Raised for invalid zones or capacities.
        """
        sections = {section.id: section for section in document.sections}
        zones = {zone.id: zone for zone in self.zones}
        counts: dict[str, int] = {}
        for layout in document.style.section_layout:
            section = sections.get(layout.section_id)
            zone = zones.get(layout.zone)
            if section is None or zone is None or section.kind not in zone.accepted_section_kinds:
                raise ResumeDomainError("resume.template_incompatible", "section layout references an incompatible zone")
            counts[zone.id] = counts.get(zone.id, 0) + 1
            if zone.max_sections is not None and counts[zone.id] > zone.max_sections:
                raise ResumeDomainError("resume.template_incompatible", "template zone capacity was exceeded")

    def default_style(self) -> ResumeStyleIntent:
        """@brief 生成符合模板的最小样式意图 / Build a minimal compatible style intent.

        @return 使用 manifest 首个可用 token 的样式 / Style using the first available manifest tokens.
        @raise ResumeDomainError manifest 本身缺少必需能力时抛出 / Raised for incomplete policies.
        """
        if not self.supported_page_sizes or not self.font_family_tokens or not self.date_format_tokens or not self.bullet_style_tokens:
            raise ResumeDomainError("resume.template_invalid", "template policy lacks required capabilities")
        measurement = Measurement(18.0, MeasurementUnit.MM)
        color = ColorValue(ColorSpace.SRGB_HEX, "#1A1A1A")
        settings = {rule.key: deepcopy(rule.default) for rule in self.settings if rule.visible_when is None}
        return ResumeStyleIntent(
            page=ResumePageIntent(min(self.supported_page_sizes, key=lambda item: item.value), None, None, PageOrientation.PORTRAIT, PageInsets(measurement, measurement, measurement, measurement), None, False),
            typography=TypographyIntent(min(self.font_family_tokens), 10.5, 1.25, 1.2, 0.0),
            palette=PaletteIntent(color, color, color, ColorValue(ColorSpace.SRGB_HEX, "#666666"), ColorValue(ColorSpace.SRGB_HEX, "#FFFFFF")),
            density=0.5,
            date_format_token=min(self.date_format_tokens),
            bullet_style_token=min(self.bullet_style_tokens),
            template_settings=settings,
        )


@dataclass(frozen=True, slots=True)
class ResumeDocument:
    """@brief Workspace 所有的权威 Resume SIR / Workspace-owned authoritative Resume SIR."""

    meta: ResourceMeta[ResumeId]
    workspace_id: WorkspaceId
    title: str
    locale: str
    profile: ResumeProfile
    sections: tuple[ResumeSection, ...]
    template: TemplateRef
    style: ResumeStyleIntent
    knowledge_source_id: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验 Resume 全局不变量 / Validate global Resume invariants.

        @raise ResumeDomainError 跨 section ID 重复或字段超界时抛出 / Raised for invalid documents.
        """
        _require_id(self.meta.id, "resume id")
        _require_id(self.workspace_id, "workspace id")
        _required_text(self.title, 300, "resume title")
        if _LOCALE.fullmatch(self.locale) is None:
            raise ResumeDomainError("resume.invalid_locale", "resume locale is invalid")
        if self.knowledge_source_id is not None:
            _require_id(self.knowledge_source_id, "knowledge source id")
        if len(self.sections) > 100:
            raise ResumeDomainError("resume.invalid_document", "resume has too many sections")
        section_ids = [section.id for section in self.sections]
        item_ids = [item.id for section in self.sections for item in section.items]
        contact_ids = [contact.id for contact in self.profile.contacts]
        entity_ids = [str(self.meta.id), *section_ids, *item_ids, *contact_ids]
        if len(set(entity_ids)) != len(entity_ids):
            raise ResumeDomainError(
                "resume.duplicate_entity_id",
                "all operation-addressable entity IDs must be globally unique",
            )

    def summary(self) -> ResumeSummary:
        """@brief 投影列表摘要 / Project a list summary.

        @return 不包含正文的 Resume 摘要 / Resume summary without body content.
        """
        return ResumeSummary(self.meta, self.workspace_id, self.title, self.locale, self.template)


@dataclass(frozen=True, slots=True)
class ResumeSummary:
    """@brief Resume 集合项投影 / Resume collection-item projection."""

    meta: ResourceMeta[ResumeId]
    workspace_id: WorkspaceId
    title: str
    locale: str
    template: TemplateRef


@dataclass(frozen=True, slots=True)
class ResumeRevision:
    """@brief 不可变 Resume revision 快照 / Immutable Resume revision snapshot."""

    resume_id: ResumeId
    revision: int
    created_at: datetime
    created_by: UserId
    document: ResumeDocument

    def __post_init__(self) -> None:
        """@brief 校验 revision 与快照一致 / Validate revision-snapshot consistency.

        @raise ResumeDomainError revision 不一致时抛出 / Raised for inconsistent snapshots.
        """
        if self.revision < 1 or self.document.meta.id != self.resume_id or self.document.meta.revision != self.revision:
            raise ResumeDomainError("resume.invalid_revision", "revision snapshot is inconsistent")
        _require_aware(self.created_at, "revision created_at")
        _require_id(self.created_by, "revision actor id")

    def summary(self) -> ResumeRevisionSummary:
        """@brief 投影 revision 摘要 / Project a revision summary.

        @return 不包含 SIR 的 revision 摘要 / Revision summary without the SIR.
        """
        return ResumeRevisionSummary(self.resume_id, self.revision, self.created_at, self.created_by)


@dataclass(frozen=True, slots=True)
class ResumeRevisionSummary:
    """@brief Resume revision 列表项 / Resume revision list item."""

    resume_id: ResumeId
    revision: int
    created_at: datetime
    created_by: UserId


@dataclass(frozen=True, slots=True)
class ChangeTarget:
    """@brief 用于安全 rebase 的稳定变更目标 / Stable change target for safe rebasing."""

    entity_id: str
    field_path: tuple[str, ...] = ()

    def overlaps(self, other: ChangeTarget) -> bool:
        """@brief 判断两个变更目标是否重叠 / Test whether two change targets overlap.

        @param other 另一目标 / Other target.
        @return entity 相同且 path 相同或互为前缀时为真 / True for equal or prefix paths on one entity.
        """
        common = min(len(self.field_path), len(other.field_path))
        return self.entity_id == other.entity_id and self.field_path[:common] == other.field_path[:common]


@dataclass(frozen=True, slots=True)
class ResumeConflict:
    """@brief 契约化 operation 冲突 / Contract-shaped operation conflict."""

    operation_id: ResumeOperationId
    code: str
    entity_id: str | None
    field_path: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResumeOperationOutcome:
    """@brief API v2 ResumeOperationResult 的领域投影 / Domain projection of API v2 ResumeOperationResult.

    @param resume 提交后的权威 Resume / Authoritative Resume after commit.
    @param applied_operation_ids 已接受的 operation IDs / Accepted operation IDs.
    @param conflicts 成功结果通常为空的结构化冲突 / Structured conflicts, normally empty on success.
    @param render_job_ref 可选渲染 Job 引用 / Optional render-job reference.
    """

    resume: ResumeDocument
    applied_operation_ids: tuple[ResumeOperationId, ...]
    conflicts: tuple[ResumeConflict, ...] = ()
    render_job_ref: ResourceRef | None = None


@dataclass(frozen=True, slots=True)
class SetResumeField:
    """@brief 按稳定 entity ID 设置字段 / Set a field by stable entity ID."""

    operation_id: ResumeOperationId
    entity_id: str
    field_path: tuple[str, ...]
    value: JsonValue
    op: str = field(default="set_field", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 set-field operation / Validate a set-field operation.

        @raise ResumeDomainError ID 或 path 无效时抛出 / Raised for invalid IDs or paths.
        """
        _validate_operation_id(self.operation_id)
        _require_id(self.entity_id, "operation entity id")
        _validate_field_path(self.field_path)


@dataclass(frozen=True, slots=True)
class UpsertResumeSection:
    """@brief 插入或替换 section / Insert or replace a section."""

    operation_id: ResumeOperationId
    section: ResumeSection
    after_section_id: str | None
    op: str = field(default="upsert_section", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 upsert-section operation / Validate an upsert-section operation."""
        _validate_operation_id(self.operation_id)
        if self.after_section_id is not None:
            _require_id(self.after_section_id, "section anchor id")


@dataclass(frozen=True, slots=True)
class UpsertResumeItem:
    """@brief 在 section 内插入或替换 item / Insert or replace an item in a section."""

    operation_id: ResumeOperationId
    section_id: str
    item: ResumeItem
    after_item_id: str | None
    op: str = field(default="upsert_item", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 upsert-item operation / Validate an upsert-item operation."""
        _validate_operation_id(self.operation_id)
        _require_id(self.section_id, "section id")
        if self.after_item_id is not None:
            _require_id(self.after_item_id, "item anchor id")


@dataclass(frozen=True, slots=True)
class RemoveResumeEntity:
    """@brief 按稳定 ID 移除 entity / Remove an entity by stable ID."""

    operation_id: ResumeOperationId
    entity_kind: EntityKind
    entity_id: str
    op: str = field(default="remove_entity", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 remove-entity operation / Validate a remove-entity operation."""
        _validate_operation_id(self.operation_id)
        _require_id(self.entity_id, "entity id")


@dataclass(frozen=True, slots=True)
class MoveResumeEntity:
    """@brief 以稳定锚点移动 entity / Move an entity using stable anchors."""

    operation_id: ResumeOperationId
    entity_kind: EntityKind
    entity_id: str
    parent_id: str | None
    after_id: str | None
    op: str = field(default="move_entity", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 move-entity operation / Validate a move-entity operation.

        @raise ResumeDomainError section 携带 parent 或 item 缺少 parent 时抛出 / Raised for invalid parent semantics.
        """
        _validate_operation_id(self.operation_id)
        _require_id(self.entity_id, "entity id")
        if self.entity_kind is EntityKind.SECTION and self.parent_id is not None:
            raise ResumeDomainError("resume.invalid_operation", "section moves require a null parent_id")
        if self.entity_kind is EntityKind.ITEM and self.parent_id is None:
            raise ResumeDomainError("resume.invalid_operation", "item moves require a destination parent_id")
        if self.parent_id is not None:
            _require_id(self.parent_id, "parent id")
        if self.after_id is not None:
            _require_id(self.after_id, "anchor id")
        if self.after_id == self.entity_id:
            raise ResumeDomainError("resume.invalid_operation", "an entity cannot be positioned after itself")


@dataclass(frozen=True, slots=True)
class SetResumeTemplate:
    """@brief 原子更换模板与 setting / Atomically replace template and settings."""

    operation_id: ResumeOperationId
    template: TemplateRef
    settings: Mapping[str, JsonValue]
    op: str = field(default="set_template", init=False)

    def __post_init__(self) -> None:
        """@brief 校验 set-template operation / Validate a set-template operation."""
        _validate_operation_id(self.operation_id)
        if len(self.settings) > 100:
            raise ResumeDomainError("resume.invalid_operation", "template settings exceed contract limits")


type ResumeOperation = SetResumeField | UpsertResumeSection | UpsertResumeItem | RemoveResumeEntity | MoveResumeEntity | SetResumeTemplate
"""@brief 六种 v2 Resume operation 的穷尽 union / Exhaustive union of the six v2 Resume operations."""


@dataclass(frozen=True, slots=True)
class ResumeOperationBatch:
    """@brief 单事务 Resume operation batch / Single-transaction Resume operation batch."""

    client_batch_id: ResumeBatchId
    base_revision: int
    conflict_strategy: ConflictStrategy
    operations: tuple[ResumeOperation, ...]
    render_hint: RenderHint

    def __post_init__(self) -> None:
        """@brief 拒绝无效批次与重复 operation ID / Reject invalid batches and duplicate operation IDs.

        @raise ResumeDomainError 批次超界或 ID 重复时抛出 / Raised for invalid batches.
        """
        _require_id(self.client_batch_id, "client batch id")
        if self.base_revision < 1 or not 1 <= len(self.operations) <= 200:
            raise ResumeDomainError("resume.invalid_operation_batch", "operation batch is outside contract limits")
        ids = [operation.operation_id for operation in self.operations]
        if len(set(ids)) != len(ids):
            raise ResumeDomainError("resume.duplicate_operation_id", "operation IDs must be unique within a batch")

    def fingerprint(self) -> str:
        """@brief 计算与 JSON 密编码无关的请求指纹 / Compute an encoding-independent request fingerprint.

        @return SHA-256 小写十六进制摘要 / Lowercase SHA-256 hexadecimal digest.
        """
        payload: dict[str, JsonValue] = {
            "client_batch_id": str(self.client_batch_id),
            "base_revision": self.base_revision,
            "conflict_strategy": self.conflict_strategy.value,
            "operations": [_operation_payload(operation) for operation in self.operations],
            "render_hint": self.render_hint.value,
        }
        return sha256(_canonical_json(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class OperationLedgerEntry:
    """@brief 持久化 operation 去重条目 / Persisted operation-deduplication entry."""

    operation_id: ResumeOperationId
    fingerprint: str
    applied_revision: int


@dataclass(frozen=True, slots=True)
class RevisionChange:
    """@brief 一个 revision 的变更目标集 / Change-target set for one revision."""

    revision: int
    targets: frozenset[ChangeTarget]


@dataclass(frozen=True, slots=True)
class ResumeAggregateChange:
    """@brief 一次原子聚合变更的完整产物 / Complete product of one atomic aggregate change."""

    aggregate: ResumeAggregate
    revision: ResumeRevision | None
    applied_operation_ids: tuple[ResumeOperationId, ...]
    deduplicated_operation_ids: tuple[ResumeOperationId, ...]


@dataclass(frozen=True, slots=True)
class ResumeAggregate:
    """@brief 带 operation ledger 与 rebase 因果记录的 Resume 聚合 / Resume aggregate with dedup and rebase history."""

    document: ResumeDocument
    operation_ledger: tuple[OperationLedgerEntry, ...] = ()
    revision_changes: tuple[RevisionChange, ...] = ()

    @classmethod
    def create(cls, document: ResumeDocument, actor_id: UserId) -> tuple[ResumeAggregate, ResumeRevision]:
        """@brief 创建首个不可变 revision / Create the first immutable revision.

        @param document revision=1 的新 Resume / New Resume at revision one.
        @param actor_id 创建者 / Creating actor.
        @return 新聚合与 revision 快照 / New aggregate and revision snapshot.
        @raise ResumeDomainError document 不是首版时抛出 / Raised unless revision is one.
        """
        if document.meta.revision != 1:
            raise ResumeDomainError("resume.invalid_revision", "new resume must start at revision one")
        aggregate = cls(document)
        return aggregate, ResumeRevision(document.meta.id, 1, document.meta.created_at, actor_id, document)

    def update_metadata(self, *, title: str | None, locale: str | None, at: datetime, actor_id: UserId) -> ResumeAggregateChange:
        """@brief 修改 Resume metadata 并产生 revision / Update Resume metadata and produce a revision.

        @param title 可选目标标题 / Optional target title.
        @param locale 可选目标 locale / Optional target locale.
        @param at 修改时刻 / Modification instant.
        @param actor_id 修改者 / Modifying actor.
        @return 原子变更产物 / Atomic change product.
        """
        target_title = self.document.title if title is None else title
        target_locale = self.document.locale if locale is None else locale
        if target_title == self.document.title and target_locale == self.document.locale:
            raise ResumeDomainError("resume.patch_noop", "resume metadata patch must change a field")
        updated = replace(self.document, meta=self.document.meta.advance(at), title=target_title, locale=target_locale)
        targets = frozenset(
            target
            for changed, target in (
                (target_title != self.document.title, ChangeTarget(str(self.document.meta.id), ("title",))),
                (target_locale != self.document.locale, ChangeTarget(str(self.document.meta.id), ("locale",))),
            )
            if changed
        )
        aggregate = replace(self, document=updated, revision_changes=(*self.revision_changes, RevisionChange(updated.meta.revision, targets)))
        revision = ResumeRevision(updated.meta.id, updated.meta.revision, at, actor_id, updated)
        return ResumeAggregateChange(aggregate, revision, (), ())

    def apply_batch(self, batch: ResumeOperationBatch, *, at: datetime, actor_id: UserId, template_policies: Mapping[TemplateRef, TemplatePolicy]) -> ResumeAggregateChange:
        """@brief 校验、去重并原子应用 operation batch / Validate, deduplicate, and atomically apply a batch.

        @param batch 已类型化批次 / Typed operation batch.
        @param at 提交时刻 / Commit instant.
        @param actor_id 提交者 / Committing actor.
        @param template_policies 批次可能使用的不可变模板策略 / Immutable policies used by the batch.
        @return 原子聚合变更 / Atomic aggregate change.
        @raise ResumeRevisionConflict 基础 revision 无法安全 rebase 时抛出 / Raised for unsafe rebases.
        @raise ResumeBatchKeyReused operation ID 被不同输入重用时抛出 / Raised for operation-ID reuse.
        """
        _require_aware(at, "operation commit time")
        ledger = {entry.operation_id: entry for entry in self.operation_ledger}
        fresh: list[ResumeOperation] = []
        deduplicated: list[ResumeOperationId] = []
        for operation in batch.operations:
            fingerprint = _operation_fingerprint(operation)
            prior = ledger.get(operation.operation_id)
            if prior is None:
                fresh.append(operation)
            elif prior.fingerprint != fingerprint:
                raise ResumeBatchKeyReused(operation=True)
            else:
                deduplicated.append(operation.operation_id)
        targets_by_operation = {operation.operation_id: _operation_targets(operation) for operation in fresh}
        if batch.base_revision != self.document.meta.revision:
            self._ensure_rebase_is_safe(batch, targets_by_operation)
        if not fresh:
            return ResumeAggregateChange(self, None, tuple(operation.operation_id for operation in batch.operations), tuple(deduplicated))
        candidate = self.document
        for operation in fresh:
            candidate = _apply_operation(candidate, operation)
        policy = template_policies.get(candidate.template)
        if policy is None:
            raise ResumeDomainError("resume.template_not_found", "referenced template version was not found")
        candidate = replace(candidate, meta=candidate.meta.advance(at))
        policy.validate(candidate)
        revision_number = candidate.meta.revision
        new_entries = tuple(OperationLedgerEntry(operation.operation_id, _operation_fingerprint(operation), revision_number) for operation in fresh)
        changed_targets = frozenset(target for targets in targets_by_operation.values() for target in targets)
        aggregate = replace(
            self,
            document=candidate,
            operation_ledger=(*self.operation_ledger, *new_entries),
            revision_changes=(*self.revision_changes, RevisionChange(revision_number, changed_targets)),
        )
        revision = ResumeRevision(candidate.meta.id, revision_number, at, actor_id, candidate)
        return ResumeAggregateChange(
            aggregate,
            revision,
            tuple(operation.operation_id for operation in batch.operations),
            tuple(deduplicated),
        )

    def _ensure_rebase_is_safe(self, batch: ResumeOperationBatch, targets: Mapping[ResumeOperationId, frozenset[ChangeTarget]]) -> None:
        """@brief 校验一个 stale batch 可否安全 rebase / Validate whether a stale batch can be safely rebased.

        @param batch 待 rebase 批次 / Batch to rebase.
        @param targets 新 operation 目标 / Targets of fresh operations.
        @raise ResumeRevisionConflict 策略拒绝或存在目标重叠时抛出 / Raised on reject or overlap.
        """
        current = self.document.meta.revision
        if batch.base_revision >= current or batch.conflict_strategy is ConflictStrategy.REJECT:
            raise ResumeRevisionConflict(current)
        changed = frozenset(target for record in self.revision_changes if record.revision > batch.base_revision for target in record.targets)
        conflicts: list[ResumeConflict] = []
        for operation_id, operation_targets in targets.items():
            for target in operation_targets:
                if any(target.overlaps(previous) for previous in changed):
                    conflicts.append(ResumeConflict(operation_id, "resume.target_changed", target.entity_id if not target.entity_id.startswith("$") else None, target.field_path))
                    break
        if conflicts:
            raise ResumeRevisionConflict(current, conflicts)


def create_resume_document(*, resume_id: ResumeId, workspace_id: WorkspaceId, title: str, locale: str, template_policy: TemplatePolicy, created_at: datetime, full_name: str = "Untitled candidate") -> ResumeDocument:
    """@brief 从模板策略创建最小有效 Resume / Create a minimal valid Resume from a template policy.

    @param resume_id 新 Resume ID / New Resume ID.
    @param workspace_id 路径 Workspace / Path Workspace.
    @param title 标题 / Title.
    @param locale locale / Locale.
    @param template_policy 不可变模板策略 / Immutable template policy.
    @param created_at 创建时刻 / Creation instant.
    @param full_name 最小 profile 姓名 / Minimal profile full name.
    @return revision=1 的有效 Resume SIR / Valid Resume SIR at revision one.
    """
    document = ResumeDocument(
        ResourceMeta(resume_id, 1, created_at, created_at),
        workspace_id,
        title,
        locale,
        ResumeProfile(full_name),
        (),
        template_policy.ref,
        template_policy.default_style(),
    )
    template_policy.validate(document)
    return document


def clone_resume_document(source: ResumeDocument, *, resume_id: ResumeId, workspace_id: WorkspaceId, title: str, locale: str, template_policy: TemplatePolicy, created_at: datetime) -> ResumeDocument:
    """@brief 在同一 Workspace 中克隆 Resume 正文 / Clone Resume content within one Workspace.

    @param source 来源 Resume / Source Resume.
    @param resume_id 新 Resume ID / New Resume ID.
    @param workspace_id 目标 Workspace / Destination Workspace.
    @param title 请求标题 / Requested title.
    @param locale 请求 locale / Requested locale.
    @param template_policy 请求模板策略 / Requested template policy.
    @param created_at 创建时刻 / Creation instant.
    @return 新标识且无服务端派生关系的副本 / Copy with new identity and no server-derived relation.
    @raise ResumeDomainError 跨 Workspace 克隆时抛出 / Raised for cross-workspace cloning.
    """
    if source.workspace_id != workspace_id:
        raise ResumeDomainError("resume.clone_scope_mismatch", "resume cloning cannot cross workspace boundaries")
    style = replace(source.style, template_settings={rule.key: deepcopy(rule.default) for rule in template_policy.settings if rule.visible_when is None})
    document = ResumeDocument(
        ResourceMeta(resume_id, 1, created_at, created_at),
        workspace_id,
        title,
        locale,
        source.profile,
        source.sections,
        template_policy.ref,
        style,
        None,
    )
    template_policy.validate(document)
    return document


def _apply_operation(document: ResumeDocument, operation: ResumeOperation) -> ResumeDocument:
    """@brief 在不可变 SIR 上应用一个 operation / Apply one operation to immutable SIR.

    @param document 候选 SIR / Candidate SIR.
    @param operation 已类型化 operation / Typed operation.
    @return 新的候选 SIR / New candidate SIR.
    """
    match operation:
        case SetResumeField():
            return _apply_set_field(document, operation)
        case UpsertResumeSection():
            return replace(document, sections=_upsert_after(document.sections, operation.section, operation.after_section_id, lambda section: section.id))
        case UpsertResumeItem():
            section_index = _section_index(document, operation.section_id)
            section = document.sections[section_index]
            updated = replace(section, items=_upsert_after(section.items, operation.item, operation.after_item_id, lambda item: item.id))
            return replace(document, sections=_replace_at(document.sections, section_index, updated))
        case RemoveResumeEntity():
            return _remove_entity(document, operation)
        case MoveResumeEntity():
            return _move_entity(document, operation)
        case SetResumeTemplate():
            return replace(document, template=operation.template, style=replace(document.style, template_settings=deepcopy(dict(operation.settings))))


def _apply_set_field(document: ResumeDocument, operation: SetResumeField) -> ResumeDocument:
    """@brief 应用无数组索引的 set-field / Apply set-field without array indexes.

    @param document 候选 SIR / Candidate SIR.
    @param operation set-field operation / Set-field operation.
    @return 完整重验证的新 SIR / Fully revalidated new SIR.
    @raise ResumeDomainError target 不存在或 path 不允许时抛出 / Raised for absent targets or forbidden paths.
    """
    path = operation.field_path
    if operation.entity_id == document.meta.id:
        if path == ("title",):
            return replace(document, title=_as_str(operation.value, "title"))
        if path == ("locale",):
            return replace(document, locale=_as_str(operation.value, "locale"))
        if path[0] == "profile":
            return replace(document, profile=_patch_profile(document.profile, path[1:], operation.value))
        if path[0] == "style":
            return replace(document, style=_patch_style(document.style, path[1:], operation.value))
        raise ResumeDomainError("resume.field_forbidden", "field path targets server-owned or structural state")
    for section_index, section in enumerate(document.sections):
        if operation.entity_id == section.id:
            updated_section = _patch_section(section, path, operation.value)
            return replace(document, sections=_replace_at(document.sections, section_index, updated_section))
        for item_index, item in enumerate(section.items):
            if operation.entity_id == item.id:
                updated_item = _patch_item(item, path, operation.value)
                updated_section = replace(section, items=_replace_at(section.items, item_index, updated_item))
                return replace(document, sections=_replace_at(document.sections, section_index, updated_section))
    for contact_index, contact in enumerate(document.profile.contacts):
        if operation.entity_id == contact.id:
            updated_contact = _patch_contact(contact, path, operation.value)
            profile = replace(
                document.profile,
                contacts=_replace_at(
                    document.profile.contacts,
                    contact_index,
                    updated_contact,
                ),
            )
            return replace(document, profile=profile)
    raise ResumeDomainError("resume.entity_not_found", "set-field target was not found")


def _remove_entity(document: ResumeDocument, operation: RemoveResumeEntity) -> ResumeDocument:
    """@brief 移除 section 或 item / Remove a section or item.

    @param document 候选 SIR / Candidate SIR.
    @param operation remove operation / Remove operation.
    @return 新 SIR / New SIR.
    """
    if operation.entity_kind is EntityKind.SECTION:
        index = _section_index(document, operation.entity_id)
        sections = document.sections[:index] + document.sections[index + 1 :]
        layouts = tuple(layout for layout in document.style.section_layout if layout.section_id != operation.entity_id)
        return replace(document, sections=sections, style=replace(document.style, section_layout=layouts))
    for section_index, section in enumerate(document.sections):
        for item_index, item in enumerate(section.items):
            if item.id == operation.entity_id:
                updated = replace(section, items=section.items[:item_index] + section.items[item_index + 1 :])
                return replace(document, sections=_replace_at(document.sections, section_index, updated))
    raise ResumeDomainError("resume.entity_not_found", "item to remove was not found")


def _move_entity(document: ResumeDocument, operation: MoveResumeEntity) -> ResumeDocument:
    """@brief 使用稳定 ID 移动 section 或 item / Move a section or item using stable IDs.

    @param document 候选 SIR / Candidate SIR.
    @param operation move operation / Move operation.
    @return 新 SIR / New SIR.
    """
    if operation.entity_kind is EntityKind.SECTION:
        index = _section_index(document, operation.entity_id)
        entity = document.sections[index]
        remaining = document.sections[:index] + document.sections[index + 1 :]
        return replace(document, sections=_insert_after(remaining, entity, operation.after_id, lambda section: section.id))
    source_section_index = -1
    source_item_index = -1
    moving: ResumeItem | None = None
    for section_index, section in enumerate(document.sections):
        for item_index, item in enumerate(section.items):
            if item.id == operation.entity_id:
                source_section_index, source_item_index, moving = section_index, item_index, item
                break
        if moving is not None:
            break
    if moving is None or operation.parent_id is None:
        raise ResumeDomainError("resume.entity_not_found", "item to move was not found")
    target_section_index = _section_index(document, operation.parent_id)
    sections = list(document.sections)
    source = sections[source_section_index]
    sections[source_section_index] = replace(source, items=source.items[:source_item_index] + source.items[source_item_index + 1 :])
    target = sections[target_section_index]
    sections[target_section_index] = replace(target, items=_insert_after(target.items, moving, operation.after_id, lambda item: item.id))
    return replace(document, sections=tuple(sections))


def _operation_targets(operation: ResumeOperation) -> frozenset[ChangeTarget]:
    """@brief 计算 operation 的因果目标 / Compute causal targets of an operation.

    @param operation Resume operation / Resume operation.
    @return 用于冲突检测的目标集 / Targets for conflict detection.
    """
    match operation:
        case SetResumeField():
            return frozenset({ChangeTarget(operation.entity_id, operation.field_path)})
        case UpsertResumeSection():
            return frozenset({ChangeTarget(operation.section.id), ChangeTarget("$sections", ("order",))})
        case UpsertResumeItem():
            return frozenset({ChangeTarget(operation.item.id), ChangeTarget(operation.section_id, ("items", "order"))})
        case RemoveResumeEntity():
            collection = "$sections" if operation.entity_kind is EntityKind.SECTION else "$items"
            return frozenset({ChangeTarget(operation.entity_id), ChangeTarget(collection, ("order",))})
        case MoveResumeEntity():
            collection = "$sections" if operation.entity_kind is EntityKind.SECTION else "$items"
            return frozenset({ChangeTarget(operation.entity_id, ("position",)), ChangeTarget(collection, ("order",))})
        case SetResumeTemplate():
            return frozenset({ChangeTarget("$template"), ChangeTarget("$style", ("template_settings",))})


def _operation_payload(operation: ResumeOperation) -> dict[str, JsonValue]:
    """@brief 将 operation 序列化为规范 JSON 结构 / Serialize an operation into canonical JSON structure.

    @param operation Resume operation / Resume operation.
    @return 无时间与存储器细节的 payload / Payload without storage details.
    """
    base: dict[str, JsonValue] = {"operation_id": str(operation.operation_id), "op": operation.op}
    match operation:
        case SetResumeField():
            base.update(entity_id=operation.entity_id, field_path=list(operation.field_path), value=deepcopy(operation.value))
        case UpsertResumeSection():
            base.update(section=_section_payload(operation.section), after_section_id=operation.after_section_id)
        case UpsertResumeItem():
            base.update(section_id=operation.section_id, item=_item_payload(operation.item), after_item_id=operation.after_item_id)
        case RemoveResumeEntity():
            base.update(entity_kind=operation.entity_kind.value, entity_id=operation.entity_id)
        case MoveResumeEntity():
            base.update(entity_kind=operation.entity_kind.value, entity_id=operation.entity_id, parent_id=operation.parent_id, after_id=operation.after_id)
        case SetResumeTemplate():
            base.update(template={"template_id": operation.template.template_id, "version": operation.template.version}, settings=deepcopy(dict(operation.settings)))
    return base


def _operation_fingerprint(operation: ResumeOperation) -> str:
    """@brief 计算单个 operation 指纹 / Compute a single-operation fingerprint.

    @param operation Resume operation / Resume operation.
    @return SHA-256 十六进制摘要 / SHA-256 hexadecimal digest.
    """
    return sha256(_canonical_json(_operation_payload(operation))).hexdigest()


def resume_operation_fingerprint(operation: ResumeOperation) -> str:
    """@brief 返回跨 proposal/ledger 共用的 operation 指纹 / Return the operation fingerprint shared by proposals and ledgers.

    @param operation 已验证 Resume operation / Validated Resume operation.
    @return canonical SHA-256 十六进制摘要 / Canonical SHA-256 hexadecimal digest.
    @note 这是 persistence adapter 可使用的公开领域函数；不得复制私有 payload 编码。
        / Persistence adapters use this public domain function rather than duplicating the private
        payload encoding.
    """

    return _operation_fingerprint(operation)


def preview_resume_operations(
    document: ResumeDocument,
    operations: Sequence[ResumeOperation],
) -> ResumeDocument:
    """@brief 无提交副作用地验证并预演 proposal operations / Validate and preview proposal operations without commit effects.

    @param document 精确基础 revision / Exact base revision.
    @param operations 按 proposal 顺序排列的 operations / Operations in proposal order.
    @return 未推进 revision 的候选 SIR / Candidate SIR without advancing its revision.
    @raise ResumeDomainError 任一引用、字段或结构操作无效时抛出 / Raised for any invalid
        reference, field, or structural operation.
    @note 模板 manifest 的最终兼容性仍在用户接受 proposal 的原子提交中验证；本函数只消除
        无效 entity/path 特例。/ Final template-manifest compatibility remains part of the atomic
        user-acceptance transaction; this function eliminates invalid entity/path drafts.
    """

    candidate = document
    for operation in operations:
        candidate = _apply_operation(candidate, operation)
    return candidate


def _patch_profile(profile: ResumeProfile, path: tuple[str, ...], value: JsonValue) -> ResumeProfile:
    """@brief 按非数组 path 修改 profile / Patch profile through a non-array path."""
    if path == ("full_name",):
        return replace(profile, full_name=_as_str(value, "profile full_name"))
    if path == ("headline",):
        return replace(profile, headline=_as_optional_str(value, "profile headline"))
    if path == ("summary",):
        return replace(profile, summary=_parse_optional_rich_text(value))
    if path == ("contacts",):
        return replace(profile, contacts=tuple(_parse_contact(item) for item in _as_list(value, "profile contacts")))
    raise ResumeDomainError("resume.field_forbidden", "profile field path is unsupported")


def _patch_section(section: ResumeSection, path: tuple[str, ...], value: JsonValue) -> ResumeSection:
    """@brief 修改 section 的可变标量字段 / Patch mutable scalar section fields."""
    if path == ("title",):
        return replace(section, title=_as_str(value, "section title"))
    if path == ("visible",):
        return replace(section, visible=_as_bool(value, "section visible"))
    if path == ("content",):
        return replace(section, content=_parse_optional_rich_text(value))
    raise ResumeDomainError("resume.field_forbidden", "section identity, kind, and items require structural operations")


def _patch_item(item: ResumeItem, path: tuple[str, ...], value: JsonValue) -> ResumeItem:
    """@brief 修改 item 的可变字段 / Patch mutable item fields."""
    if len(path) != 1:
        raise ResumeDomainError("resume.field_forbidden", "nested item paths are unsupported")
    field_name = path[0]
    if field_name in {"id", "kind"}:
        raise ResumeDomainError("resume.field_forbidden", "item identity and kind are immutable")
    if field_name == "title":
        return replace(item, title=_as_optional_str(value, "item title"))
    if field_name == "subtitle":
        return replace(item, subtitle=_as_optional_str(value, "item subtitle"))
    if field_name == "organization":
        return replace(item, organization=_as_optional_str(value, "item organization"))
    if field_name == "location":
        return replace(item, location=_as_optional_str(value, "item location"))
    if field_name == "url":
        return replace(item, url=_as_optional_str(value, "item url"))
    if field_name == "visible":
        return replace(item, visible=_as_bool(value, "item visible"))
    if field_name == "date_range":
        return replace(item, date_range=_parse_optional_date_range(value))
    if field_name == "summary":
        return replace(item, summary=_parse_optional_rich_text(value))
    if field_name == "highlights":
        return replace(item, highlights=tuple(_parse_rich_text(entry) for entry in _as_list(value, "item highlights")))
    if field_name == "skills":
        return replace(
            item,
            skills=tuple(
                _as_str(entry, "item skills")
                for entry in _as_list(value, "item skills")
            ),
        )
    if field_name == "tags":
        return replace(
            item,
            tags=tuple(
                _as_str(entry, "item tags") for entry in _as_list(value, "item tags")
            ),
        )
    raise ResumeDomainError("resume.field_forbidden", "item field path is unsupported")


def _patch_contact(contact: ContactMethod, path: tuple[str, ...], value: JsonValue) -> ContactMethod:
    """@brief 修改 contact 的可变字段 / Patch mutable contact fields."""
    if path == ("kind",):
        return replace(contact, kind=ContactKind(_as_str(value, "contact kind")))
    if path == ("label",):
        return replace(contact, label=_as_optional_str(value, "contact label"))
    if path == ("value",):
        return replace(contact, value=_as_str(value, "contact value"))
    if path == ("url",):
        return replace(contact, url=_as_optional_str(value, "contact URL"))
    raise ResumeDomainError("resume.field_forbidden", "contact ID is immutable and nested paths are unsupported")


def _patch_style(style: ResumeStyleIntent, path: tuple[str, ...], value: JsonValue) -> ResumeStyleIntent:
    """@brief 修改 style 树的稳定字段 path / Patch a stable field path in the style tree."""
    if not path:
        raise ResumeDomainError("resume.field_forbidden", "style root replacement is unsupported")
    payload = _style_payload(style)
    _set_object_path(payload, path, value)
    return _parse_style(payload)


def _section_index(document: ResumeDocument, section_id: str) -> int:
    """@brief 按 ID 查找 section 索引 / Find a section index by ID."""
    for index, section in enumerate(document.sections):
        if section.id == section_id:
            return index
    raise ResumeDomainError("resume.entity_not_found", "section was not found")


def _replace_at[T](values: tuple[T, ...], index: int, value: T) -> tuple[T, ...]:
    """@brief 不可变替换 tuple 单项 / Immutably replace one tuple item."""
    return (*values[:index], value, *values[index + 1 :])


def _upsert_after[T](
    values: tuple[T, ...],
    value: T,
    after_id: str | None,
    identity: Callable[[T], str],
) -> tuple[T, ...]:
    """@brief 按稳定 ID upsert 并定位 / Upsert and position by stable ID.

    @param values 有序值 / Ordered values.
    @param value 待 upsert 的值 / Value to upsert.
    @param after_id 稳定锚点 / Stable anchor.
    @param identity 稳定 ID 投影 / Stable-ID projection.
    @return 更新后的 tuple / Updated tuple.
    """
    remaining = tuple(item for item in values if identity(item) != identity(value))
    return _insert_after(remaining, value, after_id, identity)


def _insert_after[T](
    values: tuple[T, ...],
    value: T,
    after_id: str | None,
    identity: Callable[[T], str],
) -> tuple[T, ...]:
    """@brief 在稳定锚点后插入 tuple 项 / Insert a tuple item after a stable anchor."""
    if after_id is None:
        return (value, *values)
    for index, item in enumerate(values):
        if identity(item) == after_id:
            return (*values[: index + 1], value, *values[index + 1 :])
    raise ResumeDomainError("resume.anchor_not_found", "operation anchor was not found")


def _set_object_path(payload: dict[str, JsonValue], path: tuple[str, ...], value: JsonValue) -> None:
    """@brief 只穿越 object 设置 JSON path / Set a JSON path traversing objects only."""
    cursor = payload
    for part in path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            raise ResumeDomainError("resume.invalid_field_path", "field path crosses a non-object")
        cursor = child
    if path[-1] not in cursor:
        raise ResumeDomainError("resume.invalid_field_path", "field path does not exist")
    cursor[path[-1]] = deepcopy(value)


def _style_payload(style: ResumeStyleIntent) -> dict[str, JsonValue]:
    """@brief 投影 style 为 JSON 树 / Project style into a JSON tree."""
    return {
        "style_contract_version": style.style_contract_version,
        "page": {
            "size": style.page.size.value,
            "custom_width": _measurement_payload(style.page.custom_width) if style.page.custom_width else None,
            "custom_height": _measurement_payload(style.page.custom_height) if style.page.custom_height else None,
            "orientation": style.page.orientation.value,
            "margins": {
                "top": _measurement_payload(style.page.margins.top),
                "right": _measurement_payload(style.page.margins.right),
                "bottom": _measurement_payload(style.page.margins.bottom),
                "left": _measurement_payload(style.page.margins.left),
            },
            "max_pages": style.page.max_pages,
            "show_page_numbers": style.page.show_page_numbers,
        },
        "typography": {
            "font_family_token": style.typography.font_family_token,
            "base_size_pt": style.typography.base_size_pt,
            "line_height": style.typography.line_height,
            "heading_scale": style.typography.heading_scale,
            "letter_spacing_em": style.typography.letter_spacing_em,
        },
        "palette": {
            "primary": _color_payload(style.palette.primary),
            "secondary": _color_payload(style.palette.secondary),
            "text": _color_payload(style.palette.text),
            "muted_text": _color_payload(style.palette.muted_text),
            "background": _color_payload(style.palette.background),
        },
        "density": style.density,
        "date_format_token": style.date_format_token,
        "bullet_style_token": style.bullet_style_token,
        "section_layout": [
            {
                "section_id": layout.section_id,
                "zone": layout.zone,
                "keep_together": layout.keep_together,
                "page_break_before": layout.page_break_before,
                "compactness": layout.compactness,
                "heading_style_token": layout.heading_style_token,
            }
            for layout in style.section_layout
        ],
        "template_settings": deepcopy(dict(style.template_settings)),
        "extensions": deepcopy(dict(style.extensions)),
    }


def _measurement_payload(value: Measurement) -> dict[str, JsonValue]:
    """@brief 投影 Measurement 为 JSON object / Project a Measurement into a JSON object.

    @param value 测量值 / Measurement value.
    @return 契约 JSON object / Contract JSON object.
    """
    return {"value": value.value, "unit": value.unit.value}


def _color_payload(value: ColorValue) -> dict[str, JsonValue]:
    """@brief 投影 ColorValue 为 JSON object / Project a ColorValue into a JSON object.

    @param value 颜色值 / Color value.
    @return 契约 JSON object / Contract JSON object.
    """
    return {"space": value.space.value, "value": value.value}


def _parse_style(value: JsonValue) -> ResumeStyleIntent:
    """@brief 从 JSON 树重新构造并验证 style / Rebuild and validate style from a JSON tree."""
    payload = _as_dict(value, "style")
    page = _as_dict(payload.get("page"), "style page")
    margins = _as_dict(page.get("margins"), "page margins")
    typography = _as_dict(payload.get("typography"), "typography")
    palette = _as_dict(payload.get("palette"), "palette")
    return ResumeStyleIntent(
        page=ResumePageIntent(
            PageSize(_as_str(page.get("size"), "page size")),
            _parse_optional_measurement(page.get("custom_width")),
            _parse_optional_measurement(page.get("custom_height")),
            PageOrientation(_as_str(page.get("orientation"), "page orientation")),
            PageInsets(*(_parse_measurement(margins.get(side)) for side in ("top", "right", "bottom", "left"))),
            _as_optional_int(page.get("max_pages"), "max pages"),
            _as_bool(page.get("show_page_numbers"), "show page numbers"),
        ),
        typography=TypographyIntent(
            _as_str(typography.get("font_family_token"), "font token"),
            _as_number(typography.get("base_size_pt"), "base size"),
            _as_number(typography.get("line_height"), "line height"),
            _as_number(typography.get("heading_scale"), "heading scale"),
            _as_number(typography.get("letter_spacing_em"), "letter spacing"),
        ),
        palette=PaletteIntent(*(_parse_color(palette.get(name)) for name in ("primary", "secondary", "text", "muted_text", "background"))),
        density=_as_number(payload.get("density"), "density"),
        date_format_token=_as_str(payload.get("date_format_token"), "date format token"),
        bullet_style_token=_as_str(payload.get("bullet_style_token"), "bullet style token"),
        section_layout=tuple(_parse_section_layout(item) for item in _as_list(payload.get("section_layout"), "section layout")),
        template_settings=_as_dict(payload.get("template_settings"), "template settings"),
        extensions=_as_dict(payload.get("extensions"), "extensions"),
        style_contract_version=_as_str(payload.get("style_contract_version"), "style contract version"),
    )


def _section_payload(section: ResumeSection) -> dict[str, JsonValue]:
    """@brief 投影 section 为指纹 payload / Project a section for fingerprinting."""
    return {"id": section.id, "kind": section.kind.value, "title": section.title, "visible": section.visible, "content": _rich_text_payload(section.content), "items": [_item_payload(item) for item in section.items]}


def _item_payload(item: ResumeItem) -> dict[str, JsonValue]:
    """@brief 投影 item 为指纹 payload / Project an item for fingerprinting."""
    end: JsonValue = "present" if item.date_range and item.date_range.present else item.date_range.end.value if item.date_range and item.date_range.end else None
    date_range: JsonValue = None if item.date_range is None else {"start": item.date_range.start.value if item.date_range.start else None, "end": end}
    return {
        "id": item.id,
        "kind": item.kind.value,
        "title": item.title,
        "subtitle": item.subtitle,
        "organization": item.organization,
        "location": item.location,
        "date_range": date_range,
        "summary": _rich_text_payload(item.summary),
        "highlights": [_rich_text_payload(value) for value in item.highlights],
        "skills": list(item.skills),
        "tags": list(item.tags),
        "visible": item.visible,
        "url": item.url,
    }


def _rich_text_payload(value: RichText | None) -> JsonValue:
    """@brief 投影可空 RichText / Project optional RichText."""
    if value is None:
        return None
    return {"text": value.text, "marks": [{"start": mark.start, "end": mark.end, "kind": mark.kind.value, "href": mark.href} for mark in value.marks]}


def _parse_rich_text(value: JsonValue) -> RichText:
    """@brief 从 JSON 值解析 RichText / Parse RichText from a JSON value."""
    payload = _as_dict(value, "rich text")
    _require_object_shape(payload, {"text", "marks"}, set(), "rich text")
    marks: list[TextMark] = []
    for raw_mark in _as_list(payload.get("marks"), "text marks"):
        mark = _as_dict(raw_mark, "text mark")
        _require_object_shape(
            mark,
            {"start", "end", "kind"},
            {"href"},
            "text mark",
        )
        marks.append(TextMark(_as_int(mark.get("start"), "mark start"), _as_int(mark.get("end"), "mark end"), TextMarkKind(_as_str(mark.get("kind"), "mark kind")), _as_optional_str(mark.get("href"), "mark href")))
    return RichText(_as_str(payload.get("text"), "rich text"), tuple(marks))


def _parse_optional_rich_text(value: JsonValue) -> RichText | None:
    """@brief 解析可空 RichText / Parse optional RichText."""
    return None if value is None else _parse_rich_text(value)


def _parse_contact(value: JsonValue) -> ContactMethod:
    """@brief 从 JSON 值解析 contact / Parse a contact from JSON."""
    payload = _as_dict(value, "contact")
    _require_object_shape(
        payload,
        {"id", "kind", "label", "value", "url"},
        set(),
        "contact",
    )
    return ContactMethod(_as_str(payload.get("id"), "contact id"), ContactKind(_as_str(payload.get("kind"), "contact kind")), _as_optional_str(payload.get("label"), "contact label"), _as_str(payload.get("value"), "contact value"), _as_optional_str(payload.get("url"), "contact URL"))


def _parse_optional_date_range(value: JsonValue) -> DateRange | None:
    """@brief 从 JSON 值解析可空 DateRange / Parse an optional DateRange from JSON."""
    if value is None:
        return None
    payload = _as_dict(value, "date range")
    _require_object_shape(payload, {"start", "end"}, set(), "date range")
    start_value = _as_optional_str(payload.get("start"), "date range start")
    end_value = _as_optional_str(payload.get("end"), "date range end")
    return DateRange(PartialDate(start_value) if start_value else None, PartialDate(end_value) if end_value and end_value != "present" else None, end_value == "present")


def _parse_measurement(value: JsonValue) -> Measurement:
    """@brief 从 JSON 值解析 Measurement / Parse a Measurement from JSON."""
    payload = _as_dict(value, "measurement")
    return Measurement(_as_number(payload.get("value"), "measurement value"), MeasurementUnit(_as_str(payload.get("unit"), "measurement unit")))


def _parse_optional_measurement(value: JsonValue) -> Measurement | None:
    """@brief 解析可空 Measurement / Parse an optional Measurement."""
    return None if value is None else _parse_measurement(value)


def _parse_color(value: JsonValue) -> ColorValue:
    """@brief 从 JSON 值解析 ColorValue / Parse a ColorValue from JSON."""
    payload = _as_dict(value, "color")
    return ColorValue(ColorSpace(_as_str(payload.get("space"), "color space")), _as_str(payload.get("value"), "color value"))


def _parse_section_layout(value: JsonValue) -> SectionLayoutIntent:
    """@brief 从 JSON 值解析 section layout / Parse section layout from JSON."""
    payload = _as_dict(value, "section layout")
    return SectionLayoutIntent(_as_str(payload.get("section_id"), "layout section id"), _as_str(payload.get("zone"), "layout zone"), _as_bool(payload.get("keep_together"), "keep together"), _as_bool(payload.get("page_break_before"), "page break"), _as_number(payload.get("compactness"), "compactness"), _as_optional_str(payload.get("heading_style_token"), "heading style token"))


def _setting_type_matches(value_type: TemplateSettingValueType, value: JsonValue) -> bool:
    """@brief 判断 JSON 值是否符合 setting 声明类型 / Test a JSON value against a setting type."""
    match value_type:
        case TemplateSettingValueType.BOOLEAN:
            return isinstance(value, bool)
        case TemplateSettingValueType.INTEGER:
            return isinstance(value, int) and not isinstance(value, bool)
        case TemplateSettingValueType.NUMBER:
            return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
        case TemplateSettingValueType.STRING:
            return isinstance(value, str)
        case TemplateSettingValueType.CHOICE:
            return True
        case TemplateSettingValueType.COLOR:
            try:
                _parse_color(value)
            except (ResumeDomainError, ValueError):
                return False
            return True
        case TemplateSettingValueType.MEASUREMENT:
            try:
                _parse_measurement(value)
            except (ResumeDomainError, ValueError):
                return False
            return True


def _canonical_json(value: JsonValue) -> bytes:
    """@brief 产生指纹专用规范 JSON 字节 / Produce canonical JSON bytes for fingerprints."""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as error:
        raise ResumeDomainError("resume.invalid_json_value", "value is not canonical JSON") from error


def _validate_operation_id(value: ResumeOperationId) -> None:
    """@brief 校验 operation ID / Validate an operation ID."""
    _require_id(value, "operation id")


def _validate_field_path(path: tuple[str, ...]) -> None:
    """@brief 校验不包数组索引的 field path / Validate a field path without array indexes."""
    if not 1 <= len(path) <= 20 or any(_FIELD_PART.fullmatch(part) is None for part in path):
        raise ResumeDomainError("resume.invalid_field_path", "field path is invalid")


def _require_id(value: str, label: str) -> None:
    """@brief 校验 v2 不透明标识 / Validate a v2 opaque identifier."""
    if _OPAQUE_ID.fullmatch(value) is None:
        raise ResumeDomainError("resume.invalid_identifier", f"{label} is invalid")


def _required_text(value: str, maximum: int, label: str) -> None:
    """@brief 校验非空字符串长度 / Validate required text length."""
    if not 1 <= len(value) <= maximum:
        raise ResumeDomainError("resume.invalid_text", f"{label} is outside contract limits")


def _optional_max(value: str | None, maximum: int, label: str) -> None:
    """@brief 校验可选字符串长度 / Validate optional text length."""
    if value is not None and len(value) > maximum:
        raise ResumeDomainError("resume.invalid_text", f"{label} is outside contract limits")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ResumeDomainError("resume.invalid_timestamp", f"{label} must be timezone-aware")


def _is_safe_link(value: str) -> bool:
    """@brief 判断 URL 是否符合 SafeLinkUrl 边界 / Test whether a URL satisfies SafeLinkUrl boundaries."""
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https", "mailto", "tel"}:
        return False
    return parsed.username is None and parsed.password is None


def _as_dict(value: JsonValue | None, label: str) -> dict[str, JsonValue]:
    """@brief 将 JSON 值缩小为 object / Narrow a JSON value to an object."""
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be an object")
    return value


def _require_object_shape(
    value: Mapping[str, JsonValue],
    required: set[str],
    optional: set[str],
    label: str,
) -> None:
    """@brief 对 operation 产生的嵌套 object 执行严格 schema 形状检查 / Strictly validate a nested object produced by an operation.

    @param value 待检查 object / Object to inspect.
    @param required 必需键 / Required keys.
    @param optional 可选键 / Optional keys.
    @param label 稳定错误标签 / Stable error label.
    @raise ResumeDomainError 缺少必需键或包含未知键时抛出 / Raised for missing or unknown keys.
    """
    keys = set(value)
    if not required <= keys or keys - required - optional:
        raise ResumeDomainError(
            "resume.invalid_operation_value",
            f"{label} has missing or unknown fields",
        )


def _as_list(value: JsonValue | None, label: str) -> list[JsonValue]:
    """@brief 将 JSON 值缩小为 array / Narrow a JSON value to an array."""
    if not isinstance(value, list):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be an array")
    return value


def _as_str(value: JsonValue | None, label: str) -> str:
    """@brief 将 JSON 值缩小为 string / Narrow a JSON value to a string."""
    if not isinstance(value, str):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be a string")
    return value


def _as_optional_str(value: JsonValue | None, label: str) -> str | None:
    """@brief 将 JSON 值缩小为 nullable string / Narrow a JSON value to a nullable string."""
    return None if value is None else _as_str(value, label)


def _as_bool(value: JsonValue | None, label: str) -> bool:
    """@brief 将 JSON 值缩小为 boolean / Narrow a JSON value to a boolean."""
    if not isinstance(value, bool):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be a boolean")
    return value


def _as_int(value: JsonValue | None, label: str) -> int:
    """@brief 将 JSON 值缩小为 integer / Narrow a JSON value to an integer."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be an integer")
    return value


def _as_optional_int(value: JsonValue | None, label: str) -> int | None:
    """@brief 将 JSON 值缩小为 nullable integer / Narrow a JSON value to a nullable integer."""
    return None if value is None else _as_int(value, label)


def _as_number(value: JsonValue | None, label: str) -> float:
    """@brief 将 JSON 值缩小为有限 number / Narrow a JSON value to a finite number."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
        raise ResumeDomainError("resume.invalid_operation_value", f"{label} must be a finite number")
    return float(value)


__all__ = [
    "ChangeTarget",
    "ColorSpace",
    "ColorValue",
    "ConflictStrategy",
    "ContactKind",
    "ContactMethod",
    "DateRange",
    "EntityKind",
    "JsonValue",
    "Measurement",
    "MeasurementUnit",
    "MoveResumeEntity",
    "OperationLedgerEntry",
    "PageInsets",
    "PageOrientation",
    "PageSize",
    "PaletteIntent",
    "PartialDate",
    "RenderHint",
    "ResourceRef",
    "ResumeAggregate",
    "ResumeAggregateChange",
    "ResumeBatchId",
    "ResumeBatchKeyReused",
    "ResumeConflict",
    "ResumeDocument",
    "ResumeDomainError",
    "ResumeId",
    "ResumeItem",
    "ResumeItemKind",
    "ResumeOperation",
    "ResumeOperationBatch",
    "ResumeOperationId",
    "ResumeOperationOutcome",
    "ResumeProfile",
    "ResumeProposalId",
    "ResumeRevision",
    "ResumeRevisionConflict",
    "ResumeRevisionSummary",
    "ResumeSection",
    "ResumeSectionKind",
    "ResumeStyleIntent",
    "ResumeSummary",
    "SectionLayoutIntent",
    "SetResumeField",
    "SetResumeTemplate",
    "TemplatePolicy",
    "TemplateRef",
    "TemplateSettingRule",
    "TemplateSettingValueType",
    "TemplateZonePolicy",
    "TextMark",
    "TextMarkKind",
    "TypographyIntent",
    "UpsertResumeItem",
    "UpsertResumeSection",
    "clone_resume_document",
    "create_resume_document",
    "preview_resume_operations",
    "resume_operation_fingerprint",
]
