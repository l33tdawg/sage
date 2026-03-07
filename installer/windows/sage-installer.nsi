; SAGE Installer for Windows
; Built with NSIS (Nullsoft Scriptable Install System)
;
; To build: makensis sage-installer.nsi
; Requires: NSIS 3.x (https://nsis.sourceforge.io)

!include "MUI2.nsh"
!include "FileFunc.nsh"

; ---- Configuration ----
!define PRODUCT_NAME "SAGE"
!define PRODUCT_FULL_NAME "SAGE — AI Memory"
!define PRODUCT_PUBLISHER "Dhillon Andrew Kannabhiran"
!define PRODUCT_WEB_SITE "https://github.com/l33tdawg/sage"
!define PRODUCT_VERSION "${VERSION}"
!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
!define PRODUCT_DIR_REGKEY "Software\SAGE"

; ---- General ----
Name "${PRODUCT_FULL_NAME}"
OutFile "SAGE-${VERSION}-Windows-Setup.exe"
InstallDir "$PROGRAMFILES64\SAGE"
InstallDirRegKey HKLM "${PRODUCT_DIR_REGKEY}" ""
RequestExecutionLevel admin
SetCompressor /SOLID lzma

; ---- MUI Settings ----
!define MUI_ABORTWARNING
!define MUI_WELCOMEFINISHPAGE_BITMAP_NOSTRETCH

!define MUI_ICON "sage.ico"
!define MUI_UNICON "sage.ico"

!define MUI_WELCOMEPAGE_TITLE "Welcome to SAGE Setup"
!define MUI_WELCOMEPAGE_TEXT "SAGE gives your AI a memory.$\r$\n$\r$\nThis wizard will install SAGE Personal on your computer. After installation, run the setup wizard to configure your personal memory node.$\r$\n$\r$\nYour data stays on your machine. No cloud. No tracking.$\r$\n$\r$\nClick Next to continue."

!define MUI_FINISHPAGE_RUN "$INSTDIR\sage-lite.exe"
!define MUI_FINISHPAGE_RUN_PARAMETERS "setup"
!define MUI_FINISHPAGE_RUN_TEXT "Run SAGE Setup Wizard"
!define MUI_FINISHPAGE_LINK "Visit SAGE on GitHub"
!define MUI_FINISHPAGE_LINK_LOCATION "${PRODUCT_WEB_SITE}"

; ---- Pages ----
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "..\..\LICENSE"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

; ---- Language ----
!insertmacro MUI_LANGUAGE "English"

; ---- Install Section ----
Section "SAGE Personal" SecMain
    SectionIn RO

    SetOutPath "$INSTDIR"

    ; Copy files
    File "sage-lite.exe"
    File "sage-cli.exe"
    File "..\..\README.md"
    File "..\..\LICENSE"

    ; Create start menu shortcuts
    CreateDirectory "$SMPROGRAMS\SAGE"
    CreateShortCut "$SMPROGRAMS\SAGE\SAGE Brain Dashboard.lnk" \
        "http://localhost:8080" "" "$INSTDIR\sage-lite.exe" 0
    CreateShortCut "$SMPROGRAMS\SAGE\Start SAGE.lnk" \
        "$INSTDIR\sage-lite.exe" "serve" "" 0
    CreateShortCut "$SMPROGRAMS\SAGE\SAGE Setup Wizard.lnk" \
        "$INSTDIR\sage-lite.exe" "setup" "" 0
    CreateShortCut "$SMPROGRAMS\SAGE\Uninstall SAGE.lnk" \
        "$INSTDIR\uninstall.exe" "" "" 0

    ; Create desktop shortcut
    CreateShortCut "$DESKTOP\SAGE.lnk" \
        "$INSTDIR\sage-lite.exe" "serve" "" 0

    ; Add to PATH
    EnVar::AddValue "PATH" "$INSTDIR"

    ; Write uninstaller
    WriteUninstaller "$INSTDIR\uninstall.exe"

    ; Write registry keys for Add/Remove Programs
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayName" "${PRODUCT_FULL_NAME}"
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\uninstall.exe"
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\sage-lite.exe"
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${VERSION}"
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "Publisher" "${PRODUCT_PUBLISHER}"
    WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "URLInfoAbout" "${PRODUCT_WEB_SITE}"
    WriteRegStr HKLM "${PRODUCT_DIR_REGKEY}" "" "$INSTDIR"

    ; Calculate installed size
    ${GetSize} "$INSTDIR" "/S=0K" $0 $1 $2
    IntFmt $0 "0x%08X" $0
    WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "EstimatedSize" "$0"
SectionEnd

; ---- Uninstall Section ----
Section "Uninstall"
    ; Remove files
    Delete "$INSTDIR\sage-lite.exe"
    Delete "$INSTDIR\sage-cli.exe"
    Delete "$INSTDIR\README.md"
    Delete "$INSTDIR\LICENSE"
    Delete "$INSTDIR\uninstall.exe"

    ; Remove shortcuts
    Delete "$SMPROGRAMS\SAGE\*.lnk"
    RMDir "$SMPROGRAMS\SAGE"
    Delete "$DESKTOP\SAGE.lnk"

    ; Remove from PATH
    EnVar::DeleteValue "PATH" "$INSTDIR"

    ; Remove install directory (only if empty)
    RMDir "$INSTDIR"

    ; Remove registry keys
    DeleteRegKey HKLM "${PRODUCT_UNINST_KEY}"
    DeleteRegKey HKLM "${PRODUCT_DIR_REGKEY}"
SectionEnd
