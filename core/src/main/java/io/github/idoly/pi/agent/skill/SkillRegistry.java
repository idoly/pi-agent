package io.github.idoly.pi.agent.skill;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Agent Skills discovery, validation, progressive disclosure, and invocation. */
public final class SkillRegistry {
    private static final long MAX_SKILL_BYTES = 2L * 1024 * 1024;
    private static final int MAX_DISCOVERY_DEPTH = 32;
    private static final int MAX_DISCOVERY_ENTRIES = 10_000;
    private static final Pattern NAME = Pattern.compile(
            "^[a-z0-9]+(?:-[a-z0-9]+)*$"
    );

    private final List<AgentSkill> skills;
    private final Map<String, AgentSkill> byName;
    private final List<String> warnings;

    private SkillRegistry(List<AgentSkill> skills, List<String> warnings) {
        this.skills = List.copyOf(skills);
        LinkedHashMap<String, AgentSkill> indexed = new LinkedHashMap<>();
        skills.forEach(skill -> indexed.put(skill.name(), skill));
        byName = Map.copyOf(indexed);
        this.warnings = List.copyOf(warnings);
    }

    public static SkillRegistry discover(SkillDiscoveryOptions options) {
        Objects.requireNonNull(options, "options");
        ArrayList<Root> roots = new ArrayList<>();
        if (options.discoverDefaults()) {
            roots.add(new Root(
                    options.home().resolve(".pi/agent/skills"),
                    AgentSkill.Scope.GLOBAL, true
            ));
            roots.add(new Root(
                    options.home().resolve(".agents/skills"),
                    AgentSkill.Scope.GLOBAL, false
            ));
            if (options.projectTrusted()) {
                roots.add(new Root(
                        options.cwd().resolve(".pi/skills"),
                        AgentSkill.Scope.PROJECT, true
                ));
                for (Path directory : ancestors(options.cwd())) {
                    roots.add(new Root(
                            directory.resolve(".agents/skills"),
                            AgentSkill.Scope.PROJECT, false
                    ));
                }
            }
        }
        options.packagePaths().forEach(path -> roots.add(
                new Root(path, AgentSkill.Scope.PACKAGE, true)
        ));
        options.explicitPaths().forEach(path -> roots.add(
                new Root(path, AgentSkill.Scope.EXPLICIT, true)
        ));

        ArrayList<String> warnings = new ArrayList<>();
        LinkedHashMap<String, AgentSkill> loaded = new LinkedHashMap<>();
        LinkedHashSet<Path> sources = new LinkedHashSet<>();
        for (Root root : roots) {
            for (Path candidate : candidates(root, warnings)) {
                Path source = candidate.toAbsolutePath().normalize();
                if (!sources.add(source)) continue;
                AgentSkill skill = parse(source, root.scope(), warnings);
                if (skill == null) continue;
                AgentSkill previous = loaded.putIfAbsent(skill.name(), skill);
                if (previous != null) {
                    warnings.add("Duplicate skill '" + skill.name() + "' at "
                            + source + "; keeping " + previous.source());
                }
            }
        }
        return new SkillRegistry(new ArrayList<>(loaded.values()), warnings);
    }

    public List<AgentSkill> skills() {
        return skills;
    }

    public List<String> warnings() {
        return warnings;
    }

    public Optional<AgentSkill> find(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    /** XML fragment for the always-visible progressive-disclosure prompt. */
    public String systemPromptXml() {
        StringBuilder prompt = new StringBuilder("<available_skills>\n");
        for (AgentSkill skill : skills) {
            if (skill.disableModelInvocation()) continue;
            prompt.append("  <skill>\n")
                    .append("    <name>").append(xml(skill.name()))
                    .append("</name>\n")
                    .append("    <description>")
                    .append(xml(skill.description()))
                    .append("</description>\n")
                    .append("    <location>")
                    .append(xml(skill.source().toString()))
                    .append("</location>\n")
                    .append("  </skill>\n");
        }
        return prompt.append("</available_skills>").toString();
    }

    public String contributeToSystemPrompt(String basePrompt) {
        Objects.requireNonNull(basePrompt, "basePrompt");
        String skills = systemPromptXml();
        if (skills.equals("<available_skills>\n</available_skills>")) {
            return basePrompt;
        }
        return basePrompt.isBlank()
                ? skills : basePrompt + "\n\n" + skills;
    }

    /** Full on-demand skill instructions with optional user arguments. */
    public String invoke(String name, String arguments) {
        AgentSkill skill = find(name).orElseThrow(() ->
                new IllegalArgumentException("Unknown skill: " + name)
        );
        if (arguments == null || arguments.isBlank()) {
            return skill.instructions();
        }
        return skill.instructions() + "\n\nUser: " + arguments.strip();
    }

    private static List<Path> candidates(Root root, List<String> warnings) {
        Path path = root.path();
        if (!Files.exists(path)) return List.of();
        if (Files.isRegularFile(path)) return List.of(path);
        if (!Files.isDirectory(path)) return List.of();
        ArrayList<Path> result = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(path, MAX_DISCOVERY_DEPTH)) {
            List<Path> visited = paths.limit(MAX_DISCOVERY_ENTRIES + 1L)
                    .toList();
            if (visited.size() > MAX_DISCOVERY_ENTRIES) {
                warnings.add("Skill root exceeds " + MAX_DISCOVERY_ENTRIES
                        + " filesystem entries; remaining entries were skipped: "
                        + path);
                visited = visited.subList(0, MAX_DISCOVERY_ENTRIES);
            }
            result.addAll(visited.stream()
                    .filter(Files::isRegularFile)
                    .filter(candidate -> {
                        String name = candidate.getFileName().toString();
                        return name.equals("SKILL.md")
                                || root.allowRootMarkdown()
                                && candidate.getParent().equals(path)
                                && name.toLowerCase(Locale.ROOT).endsWith(".md");
                    }).toList());
        } catch (IOException | java.io.UncheckedIOException failure) {
            Throwable cause = failure instanceof java.io.UncheckedIOException
                    ? failure.getCause() : failure;
            warnings.add("Failed to scan skill root " + path + ": "
                    + cause.getMessage());
        }
        result.sort(java.util.Comparator.comparing(
                candidate -> candidate.toString().replace('\\', '/')
        ));
        return result;
    }

    @SuppressWarnings("unchecked")
    private static AgentSkill parse(
            Path source,
            AgentSkill.Scope scope,
            List<String> discoveryWarnings
    ) {
        try {
            long size = Files.size(source);
            if (size > MAX_SKILL_BYTES) {
                discoveryWarnings.add("Skill exceeds 2 MiB and was skipped: " + source);
                return null;
            }
            String document = Files.readString(source, StandardCharsets.UTF_8);
            if (document.startsWith("\uFEFF")) {
                document = document.substring(1);
            }
            Frontmatter frontmatter = frontmatter(document);
            if (frontmatter == null) {
                discoveryWarnings.add("Skill frontmatter is missing: " + source);
                return null;
            }
            LoaderOptions loaderOptions = new LoaderOptions();
            loaderOptions.setAllowDuplicateKeys(false);
            loaderOptions.setCodePointLimit((int) MAX_SKILL_BYTES);
            Object decoded = new Yaml(new SafeConstructor(loaderOptions))
                    .load(frontmatter.yaml());
            if (!(decoded instanceof Map<?, ?> raw)) {
                discoveryWarnings.add("Skill frontmatter is not a mapping: " + source);
                return null;
            }
            Map<String, Object> values = new LinkedHashMap<>();
            raw.forEach((key, value) -> values.put(String.valueOf(key), value));
            String name = text(values.get("name"));
            String description = text(values.get("description"));
            if (description == null || description.isBlank()) {
                discoveryWarnings.add("Skill description is missing: " + source);
                return null;
            }
            if (name == null || name.isBlank()) {
                discoveryWarnings.add("Skill name is missing: " + source);
                return null;
            }
            ArrayList<String> warnings = new ArrayList<>();
            validate(name, description, text(values.get("compatibility")), warnings);
            discoveryWarnings.addAll(warnings.stream()
                    .map(warning -> source + ": " + warning).toList());
            Map<String, Object> metadata = values.get("metadata") instanceof Map<?, ?> map
                    ? stringMap(map) : Map.of();
            List<String> allowedTools = words(values.get("allowed-tools"));
            boolean disable = booleanValue(values.get("disable-model-invocation"));
            return new AgentSkill(
                    name, description, text(values.get("license")),
                    text(values.get("compatibility")), metadata, allowedTools,
                    disable, source, source.getParent(), frontmatter.body(),
                    scope, warnings
            );
        } catch (IOException | RuntimeException failure) {
            discoveryWarnings.add("Failed to load skill " + source + ": "
                    + failure.getMessage());
            return null;
        }
    }

    private static void validate(
            String name,
            String description,
            String compatibility,
            List<String> warnings
    ) {
        if (name.length() > 64) warnings.add("name exceeds 64 characters");
        if (!NAME.matcher(name).matches()) {
            warnings.add("name should contain lowercase letters, digits, and single hyphens");
        }
        if (description.length() > 1024) {
            warnings.add("description exceeds 1024 characters");
        }
        if (compatibility != null && compatibility.length() > 500) {
            warnings.add("compatibility exceeds 500 characters");
        }
    }

    private static Frontmatter frontmatter(String document) {
        if (!document.startsWith("---\n") && !document.startsWith("---\r\n")) {
            return null;
        }
        int firstEnd = document.indexOf('\n') + 1;
        int cursor = firstEnd;
        while (cursor < document.length()) {
            int end = document.indexOf('\n', cursor);
            if (end < 0) end = document.length();
            String line = document.substring(cursor, end).stripTrailing();
            if (line.equals("---")) {
                String yaml = document.substring(firstEnd, cursor);
                String body = end < document.length()
                        ? document.substring(end + 1) : "";
                return new Frontmatter(yaml, body);
            }
            cursor = end + 1;
        }
        return null;
    }

    private static List<Path> ancestors(Path cwd) {
        ArrayList<Path> values = new ArrayList<>();
        Path current = cwd;
        Path gitRoot = null;
        while (current != null) {
            values.add(current);
            if (Files.exists(current.resolve(".git"))) {
                gitRoot = current;
                break;
            }
            current = current.getParent();
        }
        if (gitRoot != null) return List.copyOf(values);
        return List.copyOf(values);
    }

    private static String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static boolean booleanValue(Object value) {
        return value instanceof Boolean bool
                ? bool : value != null && Boolean.parseBoolean(String.valueOf(value));
    }

    private static List<String> words(Object value) {
        if (value == null) return List.of();
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return Stream.of(String.valueOf(value).strip().split("\\s+"))
                .filter(word -> !word.isEmpty()).toList();
    }

    private static Map<String, Object> stringMap(Map<?, ?> map) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, value) -> result.put(String.valueOf(key), value));
        return result;
    }

    private static String xml(String value) {
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    private record Root(
            Path path,
            AgentSkill.Scope scope,
            boolean allowRootMarkdown
    ) {
    }

    private record Frontmatter(String yaml, String body) {
    }
}
