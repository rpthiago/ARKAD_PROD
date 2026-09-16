# Backup incremental semanal do coletor da VPS (betfair_live_odds.csv, >2 GB e crescendo).
# So o que e NOVO desde o ultimo backup: tail -c a partir do byte salvo, gzip com nice, scp com banda limitada.
# Nunca le o arquivo inteiro (VPS de 1 GB travou em 16/09 fazendo isso). PowerShell por causa dos inteiros de 64 bits
# (o arquivo ja passa de 2^31 bytes; o CMD nao soma isso). Memoria: vps-1gb-nao-ler-coletor-inteiro.
$Key = "C:\Users\thiag\Downloads\ssh-key-2026-07-31.key"
$Vps = "ubuntu@163.176.59.215"
$Dir = "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\backup_coletor"
$Remoto = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"
$Log = Join-Path $Dir "backup.log"
$OffFile = Join-Path $Dir "offset.txt"
Set-Location $Dir
if (-not (Test-Path $OffFile)) { "2110416332" | Set-Content $OffFile }   # fim do backup completo de 16/09/2026
[long]$Off = (Get-Content $OffFile).Trim()
$tam = & ssh -i $Key -o StrictHostKeyChecking=no -o ConnectTimeout=30 $Vps "stat -c %s $Remoto"
if (-not $tam) { "$(Get-Date -f s) VPS sem resposta" | Add-Content $Log; exit 1 }
[long]$Tam = $tam
if ($Tam -lt $Off) { "$(Get-Date -f s) arquivo rotacionado na VPS: reinicia do zero" | Add-Content $Log; $Off = 0 }
if ($Tam -le $Off) { "$(Get-Date -f s) nada novo (tam $Tam)" | Add-Content $Log; exit 0 }
$ini = $Off + 1                                                            # tail -c +N e 1-based
& ssh -i $Key -o StrictHostKeyChecking=no $Vps "nice -n 15 tail -c +$ini $Remoto | gzip -1 > /tmp/coletor_inc.gz"
$nome = "coletor_incremental_{0}_bytes_{1}_a_{2}.csv.gz" -f (Get-Date -f yyyy-MM-dd), $Off, $Tam
& scp -q -l 24000 -i $Key -o StrictHostKeyChecking=no "${Vps}:/tmp/coletor_inc.gz" (Join-Path $Dir $nome)
if ($LASTEXITCODE -ne 0) { "$(Get-Date -f s) scp falhou" | Add-Content $Log; exit 1 }
& ssh -i $Key -o StrictHostKeyChecking=no $Vps "rm -f /tmp/coletor_inc.gz"
"$Tam" | Set-Content $OffFile
"$(Get-Date -f s) ok: $nome ($([math]::Round((Get-Item (Join-Path $Dir $nome)).Length/1MB,1)) MB)" | Add-Content $Log
